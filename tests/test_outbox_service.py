import pytest
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from cqrs_ddd.backends.sqlalchemy_outbox import SQLAlchemyOutboxStorage
from cqrs_ddd.backends.sqlalchemy import SQLAlchemyUnitOfWork
from cqrs_ddd.outbox import OutboxMessage, OutboxService


@pytest.fixture
async def engine():
    import sqlite3
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}
    )
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE outbox_messages (
                id VARCHAR(36) PRIMARY KEY,
                occurred_at TIMESTAMP NOT NULL,
                type VARCHAR(100) NOT NULL,
                topic VARCHAR(255) NOT NULL,
                payload JSON NOT NULL,
                correlation_id VARCHAR(255),
                status VARCHAR(50) DEFAULT 'pending',
                retries INTEGER DEFAULT 0,
                error TEXT,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@pytest.fixture
def uow_factory(session_factory):
    return lambda: SQLAlchemyUnitOfWork(session_factory=session_factory)


@pytest.mark.asyncio
async def test_get_pending_limits_and_retries(uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)

    msgs = []
    for i in range(5):
        msg = OutboxMessage(
            id=uuid.uuid4(),
            occurred_at=datetime.now(timezone.utc),
            type="event",
            topic=f"t{i}",
            payload={"i": i},
            correlation_id=None,
            retries=(5 if i == 4 else 0)
        )
        msgs.append(msg)
        await storage.save(msg)

    # batch_size limits to 3, and message with retries=5 should be excluded
    pending = await storage.get_pending(batch_size=3)
    assert len(pending) == 3
    assert all(m.retries < 5 for m in pending)


@pytest.mark.asyncio
async def test_save_with_unit_of_work_does_not_commit(session_factory, uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)

    msg = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="tx-test",
        payload={"ok": True}
    )

    # Use an explicit session (UnitOfWork wrapping existing session)
    from unittest.mock import AsyncMock
    async with session_factory() as session:
        # Spy on commit but let it perform real commit so we can check visibility afterwards
        orig_commit = session.commit
        async def _wrapped_commit():
            await orig_commit()
        session.commit = AsyncMock(side_effect=_wrapped_commit)

        uow = SQLAlchemyUnitOfWork(session=session)
        await storage.save(msg, unit_of_work=uow)
        # save should not trigger a commit
        assert not session.commit.called
        # Now commit and it should be visible
        await session.commit()
        assert session.commit.called

    pending_after = await storage.get_pending(batch_size=10)
    assert any(m.id == msg.id for m in pending_after)


class DummyPublisher:
    def __init__(self, fail_topics=None):
        self.published = []
        self.fail_topics = set(fail_topics or [])

    async def publish(self, topic, message, correlation_id=None):
        if topic in self.fail_topics:
            raise RuntimeError("boom")
        self.published.append((topic, message, correlation_id))


@pytest.mark.asyncio
async def test_outbox_service_process_batch_success_and_failure(uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)
    pub = DummyPublisher(fail_topics={"bad"})
    service = OutboxService(storage, pub)

    msg_good = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="good",
        payload={"x": 1}
    )
    msg_bad = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="bad",
        payload={"x": 2}
    )

    await storage.save(msg_good)
    await storage.save(msg_bad)

    count = await service.process_batch(batch_size=10)
    # only the good message should be processed successfully
    assert count == 1
    assert ("good", msg_good.payload, None) in pub.published

    # bad message should be present in pending with retries incremented
    pend = await storage.get_pending(batch_size=10)
    found = [m for m in pend if m.id == msg_bad.id]
    assert found
    assert found[0].retries == 1
    assert found[0].error == "boom"


@pytest.mark.asyncio
async def test_check_dialect_flags(uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)

    class DummyBind:
        name = "postgresql"

    class DummySession:
        bind = type("B", (), {"dialect": DummyBind()})

    storage._check_dialect(DummySession())
    assert storage._supports_skip_locked is True
    assert storage._needs_uuid_str_conversion is False

    class DummyBind2:
        name = "sqlite"

    class DummySession2:
        bind = type("B", (), {"dialect": DummyBind2()})

    storage2 = SQLAlchemyOutboxStorage(uow_factory=uow_factory)
    storage2._check_dialect(DummySession2())
    assert storage2._supports_skip_locked is False
    assert storage2._needs_uuid_str_conversion is True
