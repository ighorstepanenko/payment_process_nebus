import uuid

from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import OutboxEvent, OutboxEventStatus, Payment, PaymentStatus
from app.repositories.payment import PaymentRepository


class TestCreatePayment:
    async def test_returns_202(
        self, client: AsyncClient, api_headers: dict[str, str], payment_body: dict[str, object]
    ) -> None:
        resp = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        assert resp.status_code == 202

    async def test_response_shape(
        self, client: AsyncClient, api_headers: dict[str, str], payment_body: dict[str, object]
    ) -> None:
        resp = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        data = resp.json()
        assert "payment_id" in data
        assert data["status"] == "pending"
        assert "created_at" in data

    async def test_creates_payment_in_db(
        self,
        client: AsyncClient,
        session: AsyncSession,
        api_headers: dict[str, str],
        payment_body: dict[str, object],
    ) -> None:
        resp = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        payment_id = uuid.UUID(resp.json()["payment_id"])

        repo = PaymentRepository(session)
        payment = await repo.get_by_id(payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.PENDING

    async def test_creates_outbox_event(
        self,
        client: AsyncClient,
        session: AsyncSession,
        api_headers: dict[str, str],
        payment_body: dict[str, object],
    ) -> None:
        resp = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        payment_id = uuid.UUID(resp.json()["payment_id"])

        result = await session.execute(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == payment_id)
        )
        events = list(result.scalars().all())
        assert len(events) == 1
        assert events[0].event_type == "payment.new"
        assert events[0].status == OutboxEventStatus.PENDING


class TestIdempotency:
    async def test_same_key_returns_same_payment(
        self, client: AsyncClient, api_headers: dict[str, str], payment_body: dict[str, object]
    ) -> None:
        resp1 = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        resp2 = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)

        assert resp1.json()["payment_id"] == resp2.json()["payment_id"]

    async def test_different_key_creates_new_payment(
        self, client: AsyncClient, payment_body: dict[str, object]
    ) -> None:
        headers1 = {
            "X-API-Key": settings.api_key,
            "Idempotency-Key": str(uuid.uuid4()),
        }
        headers2 = {
            "X-API-Key": settings.api_key,
            "Idempotency-Key": str(uuid.uuid4()),
        }
        resp1 = await client.post("/api/v1/payments", json=payment_body, headers=headers1)
        resp2 = await client.post("/api/v1/payments", json=payment_body, headers=headers2)

        assert resp1.json()["payment_id"] != resp2.json()["payment_id"]


class TestGetPayment:
    async def test_returns_existing_payment(
        self, client: AsyncClient, saved_payment: Payment
    ) -> None:
        resp = await client.get(
            f"/api/v1/payments/{saved_payment.id}",
            headers={"X-API-Key": settings.api_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(saved_payment.id)
        assert data["currency"] == "USD"
        assert data["status"] == "pending"

    async def test_404_for_nonexistent(self, client: AsyncClient) -> None:
        resp = await client.get(
            f"/api/v1/payments/{uuid.uuid4()}",
            headers={"X-API-Key": settings.api_key},
        )
        assert resp.status_code == 404


class TestAuth:
    async def test_no_api_key_returns_401(
        self, client: AsyncClient, payment_body: dict[str, object]
    ) -> None:
        resp = await client.post(
            "/api/v1/payments",
            json=payment_body,
            headers={"Idempotency-Key": "key"},
        )
        assert resp.status_code == 401

    async def test_wrong_api_key_returns_401(
        self, client: AsyncClient, payment_body: dict[str, object]
    ) -> None:
        resp = await client.post(
            "/api/v1/payments",
            json=payment_body,
            headers={"X-API-Key": "wrong", "Idempotency-Key": "key"},
        )
        assert resp.status_code == 401

    async def test_valid_api_key_passes(
        self, client: AsyncClient, api_headers: dict[str, str], payment_body: dict[str, object]
    ) -> None:
        resp = await client.post("/api/v1/payments", json=payment_body, headers=api_headers)
        assert resp.status_code == 202


class TestValidation:
    async def test_negative_amount_rejected(
        self, client: AsyncClient, api_headers: dict[str, str]
    ) -> None:
        body = {
            "amount": "-10.00",
            "currency": "RUB",
            "webhook_url": "https://example.com/hook",
        }
        resp = await client.post("/api/v1/payments", json=body, headers=api_headers)
        assert resp.status_code == 422

    async def test_invalid_currency_rejected(
        self, client: AsyncClient, api_headers: dict[str, str]
    ) -> None:
        body = {
            "amount": "100.00",
            "currency": "GBP",
            "webhook_url": "https://example.com/hook",
        }
        resp = await client.post("/api/v1/payments", json=body, headers=api_headers)
        assert resp.status_code == 422
