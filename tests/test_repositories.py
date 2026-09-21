import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, OutboxEventStatus, Payment, PaymentStatus
from app.repositories.outbox import OutboxRepository
from app.repositories.payment import PaymentRepository


class TestPaymentRepository:
    async def test_create_and_get_by_id(self, session: AsyncSession) -> None:
        repo = PaymentRepository(session)
        payment = await repo.create(
            amount=Decimal("250.00"),
            currency="EUR",
            description="repo test",
            metadata={"key": "val"},
            idempotency_key=str(uuid.uuid4()),
            webhook_url="https://example.com/hook",
        )
        await session.flush()

        fetched = await repo.get_by_id(payment.id)
        assert fetched is not None
        assert fetched.amount == Decimal("250.00")
        assert fetched.currency == "EUR"
        assert fetched.status == PaymentStatus.PENDING

    async def test_get_by_idempotency_key(self, session: AsyncSession) -> None:
        repo = PaymentRepository(session)
        key = str(uuid.uuid4())
        await repo.create(
            amount=Decimal("10.00"),
            currency="RUB",
            description="",
            metadata=None,
            idempotency_key=key,
            webhook_url="https://example.com/hook",
        )
        await session.flush()

        found = await repo.get_by_idempotency_key(key)
        assert found is not None
        assert found.idempotency_key == key

    async def test_get_by_idempotency_key_not_found(self, session: AsyncSession) -> None:
        repo = PaymentRepository(session)
        assert await repo.get_by_idempotency_key("nonexistent") is None

    async def test_get_by_id_not_found(self, session: AsyncSession) -> None:
        repo = PaymentRepository(session)
        assert await repo.get_by_id(uuid.uuid4()) is None

    async def test_update_status(self, session: AsyncSession) -> None:
        repo = PaymentRepository(session)
        payment = await repo.create(
            amount=Decimal("99.99"),
            currency="USD",
            description="",
            metadata=None,
            idempotency_key=str(uuid.uuid4()),
            webhook_url="https://example.com/hook",
        )
        await session.flush()

        now = datetime.now(tz=UTC)
        await repo.update_status(payment.id, PaymentStatus.SUCCEEDED, processed_at=now)
        await session.flush()

        updated = await repo.get_by_id(payment.id)
        assert updated is not None
        assert updated.status == PaymentStatus.SUCCEEDED
        assert updated.processed_at is not None


class TestOutboxRepository:
    async def test_create_event(
        self, session: AsyncSession, saved_payment: Payment
    ) -> None:
        repo = OutboxRepository(session)
        event = await repo.create(
            aggregate_id=saved_payment.id,
            event_type="payment.new",
            payload={"payment_id": str(saved_payment.id)},
        )
        await session.flush()

        assert event.status == OutboxEventStatus.PENDING
        assert event.retries == 0

    async def test_get_pending_batch(
        self, session: AsyncSession, saved_outbox_event: OutboxEvent
    ) -> None:
        repo = OutboxRepository(session)
        events = await repo.get_pending_batch(10)
        ids = [e.id for e in events]
        assert saved_outbox_event.id in ids

    async def test_mark_published(
        self, session: AsyncSession, saved_outbox_event: OutboxEvent
    ) -> None:
        repo = OutboxRepository(session)
        await repo.mark_published(saved_outbox_event.id)
        await session.flush()

        events = await repo.get_pending_batch(10)
        ids = [e.id for e in events]
        assert saved_outbox_event.id not in ids

    async def test_mark_retry_or_fail_retries(
        self, session: AsyncSession, saved_outbox_event: OutboxEvent
    ) -> None:
        repo = OutboxRepository(session)
        await repo.mark_retry_or_fail(saved_outbox_event.id, current_retries=0, max_retries=3)
        await session.flush()
        await session.refresh(saved_outbox_event)

        assert saved_outbox_event.retries == 1
        assert saved_outbox_event.status == OutboxEventStatus.PENDING

    async def test_mark_retry_or_fail_exceeds_max(
        self, session: AsyncSession, saved_outbox_event: OutboxEvent
    ) -> None:
        repo = OutboxRepository(session)
        await repo.mark_retry_or_fail(saved_outbox_event.id, current_retries=2, max_retries=3)
        await session.flush()
        await session.refresh(saved_outbox_event)

        assert saved_outbox_event.retries == 3
        assert saved_outbox_event.status == OutboxEventStatus.FAILED
