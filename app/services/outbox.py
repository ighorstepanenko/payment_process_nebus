"""Outbox publisher — polls outbox_events and publishes them to RabbitMQ."""

import asyncio
import json
import logging

import aio_pika

from app.config import settings
from app.database import async_session_factory
from app.logging_config import setup_logging
from app.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "payments"
ROUTING_KEY = "payments.new"
MAX_PUBLISH_RETRIES = 3


async def publish_pending_events(
    channel: aio_pika.abc.AbstractChannel,
) -> int:
    async with async_session_factory() as session:
        repo = OutboxRepository(session)
        events = await repo.get_pending_batch(settings.outbox_batch_size)

        if not events:
            return 0

        exchange = await channel.get_exchange(EXCHANGE_NAME)

        published = 0
        for event in events:
            try:
                message = aio_pika.Message(
                    body=json.dumps(event.payload).encode(),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(event.id),
                )
                await exchange.publish(message, routing_key=ROUTING_KEY)
                await repo.mark_published(event.id)
                published += 1
            except Exception:
                logger.exception(
                    "outbox_publish_failed",
                    extra={"event_id": str(event.id), "aggregate_id": str(event.aggregate_id)},
                )
                await repo.mark_retry_or_fail(event.id, event.retries, MAX_PUBLISH_RETRIES)

        await session.commit()
        if published:
            logger.info("outbox_batch_published", extra={"count": published})
        return published


async def ensure_topology(channel: aio_pika.abc.AbstractChannel) -> None:
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
    )

    dlx = await channel.declare_exchange(
        f"{EXCHANGE_NAME}.dlx", aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlq = await channel.declare_queue(f"{EXCHANGE_NAME}.dlq", durable=True)
    await dlq.bind(dlx, routing_key=ROUTING_KEY)

    queue = await channel.declare_queue(
        ROUTING_KEY,
        durable=True,
        arguments={
            "x-dead-letter-exchange": f"{EXCHANGE_NAME}.dlx",
            "x-dead-letter-routing-key": ROUTING_KEY,
        },
    )
    await queue.bind(exchange, routing_key=ROUTING_KEY)


async def run_outbox_publisher() -> None:
    setup_logging()
    logger.info("outbox_publisher_starting")

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        await ensure_topology(channel)

        logger.info("outbox_publisher_ready")
        while True:
            try:
                count = await publish_pending_events(channel)
                if count == 0:
                    await asyncio.sleep(settings.outbox_poll_interval_sec)
            except Exception:
                logger.exception("outbox_publisher_loop_error")
                await asyncio.sleep(settings.outbox_poll_interval_sec * 5)


if __name__ == "__main__":
    asyncio.run(run_outbox_publisher())
