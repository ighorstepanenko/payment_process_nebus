import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.id == payment_id)
        )
        return result.scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self._session.execute(
            select(Payment).where(Payment.idempotency_key == key)
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        *,
        amount: Decimal,
        currency: str,
        description: str,
        metadata: dict[str, Any] | None,
        idempotency_key: str,
        webhook_url: str,
    ) -> Payment:
        payment = Payment(
            id=uuid.uuid4(),
            amount=amount,
            currency=currency,
            description=description,
            metadata_=metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=webhook_url,
        )
        self._session.add(payment)
        return payment

    async def update_status(
        self,
        payment_id: uuid.UUID,
        status: PaymentStatus,
        processed_at: datetime,
    ) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(status=status, processed_at=processed_at)
        )
