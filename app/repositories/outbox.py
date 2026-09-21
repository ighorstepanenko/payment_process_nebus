import uuid
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, OutboxEventStatus


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        aggregate_id: uuid.UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> OutboxEvent:
        event = OutboxEvent(
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            status=OutboxEventStatus.PENDING,
        )
        self._session.add(event)
        return event

    async def get_pending_batch(self, limit: int) -> list[OutboxEvent]:
        result = await self._session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxEventStatus.PENDING)
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    async def mark_published(self, event_id: uuid.UUID) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(status=OutboxEventStatus.PUBLISHED)
        )

    async def mark_retry_or_fail(
        self, event_id: uuid.UUID, current_retries: int, max_retries: int
    ) -> None:
        new_retries = current_retries + 1
        new_status = (
            OutboxEventStatus.FAILED
            if new_retries >= max_retries
            else OutboxEventStatus.PENDING
        )
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(retries=new_retries, status=new_status)
        )
