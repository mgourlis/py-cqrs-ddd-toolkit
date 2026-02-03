import pytest
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from cqrs_ddd.backends.sqlalchemy_outbox import SQLAlchemyOutboxStorage
from cqrs_ddd.backends.sqlalchemy import SQLAlchemyUnitOfWork
from cqrs_ddd.outbox import OutboxMessage


@pytest.fixture
async def engine():
    import sqlite3

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES},
    )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
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
        """
            )
        )
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@pytest.fixture
def uow_factory(session_factory):
    return lambda: SQLAlchemyUnitOfWork(session_factory=session_factory)


@pytest.mark.asyncio
async def test_save_and_get_pending(uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)

    msg = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="t1",
        payload={"k": "v"},
        correlation_id="corr-1",
    )

    await storage.save(msg)

    pending = await storage.get_pending(batch_size=10)
    assert len(pending) == 1
    got = pending[0]
    assert got.topic == "t1"
    assert got.payload == {"k": "v"}


@pytest.mark.asyncio
async def test_mark_published_and_mark_failed(uow_factory):
    storage = SQLAlchemyOutboxStorage(uow_factory=uow_factory)

    msg = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="t2",
        payload={"x": 1},
        correlation_id=None,
    )

    await storage.save(msg)

    # mark published
    await storage.mark_published(msg.id)
    pending = await storage.get_pending(batch_size=10)
    # should be empty since it's published
    assert all(m.id != msg.id for m in pending)

    # Create a new message for retry testing
    msg2 = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=datetime.now(timezone.utc),
        type="event",
        topic="t2",
        payload={"x": 1},
    )
    await storage.save(msg2)

    await storage.mark_failed(msg2.id, "err1")
    await storage.mark_failed(msg2.id, "err2")

    pend = await storage.get_pending(batch_size=10)
    # after failures, message still eligible (retries < 5) and should appear
    found = [m for m in pend if m.id == msg2.id]
    assert found
    f = found[0]
    assert f.retries == 2
    assert f.error == "err2"
