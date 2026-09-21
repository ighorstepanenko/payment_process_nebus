"""Payment consumer — reads from payments.new queue, processes payments, sends webhooks."""

import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Any

import httpx
from faststream import FastStream
from faststream.middlewares.acknowledgement.config import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import ExchangeType

from app.config import settings
from app.database import async_session_factory
from app.logging_config import setup_logging
from app.models import PaymentStatus
from app.repositories.payment import PaymentRepository
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)

broker = RabbitBroker(settings.rabbitmq_url)

dlx_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
main_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new",
    },
)

PROCESSING_MIN_SEC = 2
PROCESSING_MAX_SEC = 5
SUCCESS_RATE = 0.9


async def send_webhook(
    webhook_url: str,
    payload: dict[str, Any],
    max_retries: int = 3,
) -> bool:
    """Send webhook notification with exponential backoff retries."""
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_sec) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.post(webhook_url, json=payload)
                response.raise_for_status()
                logger.info(
                    "webhook_sent",
                    extra={
                        "webhook_url": webhook_url,
                        "status_code": response.status_code,
                        "attempt": attempt,
                    },
                )
                return True
            except Exception:
                logger.warning(
                    "webhook_retry",
                    extra={
                        "webhook_url": webhook_url,
                        "attempt": attempt,
                        "max_attempts": max_retries,
                    },
                )
                if attempt < max_retries:
                    await asyncio.sleep(2**attempt)

    logger.error("webhook_failed", extra={"webhook_url": webhook_url})
    return False


@broker.subscriber(
    payments_queue,
    main_exchange,
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def process_payment(data: dict[str, Any]) -> None:
    event = PaymentEvent.model_validate(data)
    payment_id = event.payment_id
    logger.info("payment_processing_started", extra={"payment_id": str(payment_id)})

    delay = random.uniform(PROCESSING_MIN_SEC, PROCESSING_MAX_SEC)
    await asyncio.sleep(delay)

    succeeded = random.random() < SUCCESS_RATE
    new_status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
    now = datetime.now(tz=UTC)

    async with async_session_factory() as session:
        repo = PaymentRepository(session)
        payment = await repo.get_by_id(payment_id)

        if payment is None:
            logger.error("payment_not_found", extra={"payment_id": str(payment_id)})
            return

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                "payment_already_processed",
                extra={"payment_id": str(payment_id), "status": payment.status.value},
            )
            return

        await repo.update_status(payment_id, new_status, processed_at=now)
        await session.commit()

    logger.info(
        "payment_processing_completed",
        extra={
            "payment_id": str(payment_id),
            "status": new_status.value,
            "duration_sec": round(delay, 2),
        },
    )

    webhook_payload: dict[str, Any] = {
        "payment_id": str(payment_id),
        "status": new_status.value,
        "amount": event.amount,
        "currency": event.currency,
        "processed_at": now.isoformat(),
    }
    await send_webhook(
        event.webhook_url,
        webhook_payload,
        max_retries=settings.webhook_max_retries,
    )


app = FastStream(broker)

if __name__ == "__main__":
    setup_logging()
    asyncio.run(app.run())
