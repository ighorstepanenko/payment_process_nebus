import uuid
from collections.abc import AsyncGenerator
from decimal import Decimal

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.database import get_session
from app.main import app
from app.models import Base, OutboxEvent, OutboxEventStatus, Payment, PaymentStatus

_base, _, _db_name = settings.database_url.rpartition("/")
TEST_DB_URL = f"{_base}/payments_test"


@pytest.fixture
async def test_engine() -> AsyncGenerator[AsyncEngine, None]:
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def session(test_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with factory() as s:
        yield s
        await s.rollback()


@pytest.fixture
async def client(session: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    async def _override_session() -> AsyncGenerator[AsyncSession, None]:
        yield session

    app.dependency_overrides[get_session] = _override_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {
        "X-API-Key": settings.api_key,
        "Idempotency-Key": str(uuid.uuid4()),
        "Content-Type": "application/json",
    }


@pytest.fixture
def payment_body() -> dict[str, object]:
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order_id": 1},
        "webhook_url": "https://example.com/webhook",
    }


@pytest.fixture
async def saved_payment(session: AsyncSession) -> Payment:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("500.00"),
        currency="USD",
        description="Saved",
        metadata_=None,
        status=PaymentStatus.PENDING,
        idempotency_key=str(uuid.uuid4()),
        webhook_url="https://example.com/hook",
    )
    session.add(payment)
    await session.flush()
    return payment


@pytest.fixture
async def saved_outbox_event(session: AsyncSession, saved_payment: Payment) -> OutboxEvent:
    event = OutboxEvent(
        aggregate_id=saved_payment.id,
        event_type="payment.new",
        payload={"payment_id": str(saved_payment.id)},
        status=OutboxEventStatus.PENDING,
    )
    session.add(event)
    await session.flush()
    return event
