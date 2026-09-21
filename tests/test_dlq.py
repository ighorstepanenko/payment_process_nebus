"""DLQ integration tests — require running RabbitMQ (docker-compose)."""

import json
from collections.abc import AsyncGenerator

import aio_pika
import pytest

from app.config import settings

TEST_EXCHANGE = "payments.test"
TEST_DLX = "payments.test.dlx"
TEST_QUEUE = "payments.test.new"
TEST_DLQ = "payments.test.dlq"
TEST_ROUTING_KEY = "payments.test.new"


@pytest.fixture
async def rmq_channel() -> AsyncGenerator[aio_pika.abc.AbstractChannel, None]:
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    channel = await connection.channel()

    exchange = await channel.declare_exchange(
        TEST_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(TEST_DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    dlq = await channel.declare_queue(TEST_DLQ, durable=True)
    await dlq.bind(dlx, routing_key=TEST_ROUTING_KEY)
    queue = await channel.declare_queue(
        TEST_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": TEST_DLX,
            "x-dead-letter-routing-key": TEST_ROUTING_KEY,
        },
    )
    await queue.bind(exchange, routing_key=TEST_ROUTING_KEY)

    yield channel

    await queue.delete(if_empty=False, if_unused=False)
    await dlq.delete(if_empty=False, if_unused=False)
    await exchange.delete(if_unused=False)
    await dlx.delete(if_unused=False)
    await connection.close()


class TestDLQ:
    async def test_rejected_message_goes_to_dlq(
        self, rmq_channel: aio_pika.abc.AbstractChannel
    ) -> None:
        exchange = await rmq_channel.get_exchange(TEST_EXCHANGE)
        test_body = {"test": "dlq_message"}
        message = aio_pika.Message(
            body=json.dumps(test_body).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await exchange.publish(message, routing_key=TEST_ROUTING_KEY)

        queue = await rmq_channel.get_queue(TEST_QUEUE)
        incoming = await queue.get(timeout=5, fail=True)
        assert incoming is not None
        await incoming.reject(requeue=False)

        dlq = await rmq_channel.get_queue(TEST_DLQ)
        dlq_msg = await dlq.get(timeout=5, fail=True)
        assert dlq_msg is not None
        assert json.loads(dlq_msg.body) == test_body
        await dlq_msg.ack()

    async def test_acked_message_does_not_go_to_dlq(
        self, rmq_channel: aio_pika.abc.AbstractChannel
    ) -> None:
        exchange = await rmq_channel.get_exchange(TEST_EXCHANGE)
        message = aio_pika.Message(
            body=b'{"test": "should_not_dlq"}',
            content_type="application/json",
        )
        await exchange.publish(message, routing_key=TEST_ROUTING_KEY)

        queue = await rmq_channel.get_queue(TEST_QUEUE)
        incoming = await queue.get(timeout=5, fail=True)
        await incoming.ack()

        dlq = await rmq_channel.get_queue(TEST_DLQ)
        dlq_msg = await dlq.get(timeout=2, fail=False)
        assert dlq_msg is None
