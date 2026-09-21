from unittest.mock import AsyncMock, patch

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.models import OutboxEvent
from app.services.outbox import MAX_PUBLISH_RETRIES, publish_pending_events


def _make_test_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class TestPublishPendingEvents:
    async def test_publishes_event_and_marks_published(
        self,
        session: AsyncSession,
        test_engine: AsyncEngine,
        saved_outbox_event: OutboxEvent,
    ) -> None:
        await session.commit()

        mock_exchange = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)

        with patch(
            "app.services.outbox.async_session_factory",
            _make_test_session_factory(test_engine),
        ):
            count = await publish_pending_events(mock_channel)

        assert count >= 1
        mock_exchange.publish.assert_called()

    async def test_returns_zero_when_no_events(
        self, session: AsyncSession, test_engine: AsyncEngine
    ) -> None:
        mock_channel = AsyncMock()

        with patch(
            "app.services.outbox.async_session_factory",
            _make_test_session_factory(test_engine),
        ):
            count = await publish_pending_events(mock_channel)

        assert count == 0

    async def test_marks_failed_after_max_retries(
        self,
        session: AsyncSession,
        test_engine: AsyncEngine,
        saved_outbox_event: OutboxEvent,
    ) -> None:
        saved_outbox_event.retries = MAX_PUBLISH_RETRIES - 1
        await session.commit()

        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = Exception("RabbitMQ down")
        mock_channel = AsyncMock()
        mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)

        with patch(
            "app.services.outbox.async_session_factory",
            _make_test_session_factory(test_engine),
        ):
            count = await publish_pending_events(mock_channel)

        assert count == 0

    async def test_stays_pending_on_partial_retries(
        self,
        session: AsyncSession,
        test_engine: AsyncEngine,
        saved_outbox_event: OutboxEvent,
    ) -> None:
        saved_outbox_event.retries = 0
        await session.commit()

        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = Exception("Temporary error")
        mock_channel = AsyncMock()
        mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)

        with patch(
            "app.services.outbox.async_session_factory",
            _make_test_session_factory(test_engine),
        ):
            count = await publish_pending_events(mock_channel)

        assert count == 0
