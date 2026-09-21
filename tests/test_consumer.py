import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.consumer.worker import process_payment, send_webhook
from app.models import PaymentStatus
from app.repositories.payment import PaymentRepository

_FACTORY_PATH = "app.consumer.worker.async_session_factory"


class TestSendWebhook:
    async def test_success_on_first_attempt(self) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        with patch("app.consumer.worker.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await send_webhook("https://example.com/hook", {"status": "ok"})

        assert result is True
        mock_client.post.assert_called_once()

    async def test_retries_on_failure(self) -> None:
        with patch("app.consumer.worker.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = Exception("Connection error")
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock):
                result = await send_webhook(
                    "https://example.com/hook", {"status": "ok"}, max_retries=3
                )

        assert result is False
        assert mock_client.post.call_count == 3

    async def test_succeeds_after_retry(self) -> None:
        mock_fail_response = MagicMock()
        mock_fail_response.raise_for_status.side_effect = Exception("500")

        mock_ok_response = MagicMock()
        mock_ok_response.status_code = 200
        mock_ok_response.raise_for_status = MagicMock()

        with patch("app.consumer.worker.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = [mock_fail_response, mock_ok_response]
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            with patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock):
                result = await send_webhook(
                    "https://example.com/hook", {"status": "ok"}, max_retries=3
                )

        assert result is True
        assert mock_client.post.call_count == 2


def _make_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


def _make_event_data(payment_id: uuid.UUID) -> dict[str, Any]:
    return {
        "payment_id": str(payment_id),
        "amount": "100.50",
        "currency": "RUB",
        "description": "test",
        "metadata": None,
        "webhook_url": "https://example.com/hook",
        "idempotency_key": str(uuid.uuid4()),
    }


class TestProcessPayment:
    async def test_updates_payment_status(
        self, session: AsyncSession, test_engine: AsyncEngine
    ) -> None:
        repo = PaymentRepository(session)
        payment = await repo.create(
            amount=Decimal("100.50"),
            currency="RUB",
            description="test",
            metadata=None,
            idempotency_key=str(uuid.uuid4()),
            webhook_url="https://example.com/hook",
        )
        await session.commit()

        with (
            patch(_FACTORY_PATH, _make_factory(test_engine)),
            patch("app.consumer.worker.random.uniform", return_value=0.01),
            patch("app.consumer.worker.random.random", return_value=0.5),
            patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock),
            patch("app.consumer.worker.send_webhook", new_callable=AsyncMock) as mock_wh,
        ):
            await process_payment(_make_event_data(payment.id))

        await session.refresh(payment)
        assert payment.status == PaymentStatus.SUCCEEDED
        assert payment.processed_at is not None
        mock_wh.assert_called_once()

    async def test_skips_already_processed(
        self, session: AsyncSession, test_engine: AsyncEngine
    ) -> None:
        repo = PaymentRepository(session)
        payment = await repo.create(
            amount=Decimal("100.50"),
            currency="RUB",
            description="test",
            metadata=None,
            idempotency_key=str(uuid.uuid4()),
            webhook_url="https://example.com/hook",
        )
        now = datetime.now(tz=UTC)
        await repo.update_status(payment.id, PaymentStatus.SUCCEEDED, processed_at=now)
        await session.commit()

        with (
            patch(_FACTORY_PATH, _make_factory(test_engine)),
            patch("app.consumer.worker.random.uniform", return_value=0.01),
            patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock),
            patch("app.consumer.worker.send_webhook", new_callable=AsyncMock) as mock_wh,
        ):
            await process_payment(_make_event_data(payment.id))

        mock_wh.assert_not_called()

    async def test_handles_missing_payment(
        self, test_engine: AsyncEngine
    ) -> None:
        with (
            patch(_FACTORY_PATH, _make_factory(test_engine)),
            patch("app.consumer.worker.random.uniform", return_value=0.01),
            patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock),
            patch("app.consumer.worker.send_webhook", new_callable=AsyncMock) as mock_wh,
        ):
            await process_payment(_make_event_data(uuid.uuid4()))

        mock_wh.assert_not_called()

    async def test_failed_payment(
        self, session: AsyncSession, test_engine: AsyncEngine
    ) -> None:
        repo = PaymentRepository(session)
        payment = await repo.create(
            amount=Decimal("100.50"),
            currency="RUB",
            description="test",
            metadata=None,
            idempotency_key=str(uuid.uuid4()),
            webhook_url="https://example.com/hook",
        )
        await session.commit()

        with (
            patch(_FACTORY_PATH, _make_factory(test_engine)),
            patch("app.consumer.worker.random.uniform", return_value=0.01),
            patch("app.consumer.worker.random.random", return_value=0.95),
            patch("app.consumer.worker.asyncio.sleep", new_callable=AsyncMock),
            patch("app.consumer.worker.send_webhook", new_callable=AsyncMock),
        ):
            await process_payment(_make_event_data(payment.id))

        await session.refresh(payment)
        assert payment.status == PaymentStatus.FAILED
