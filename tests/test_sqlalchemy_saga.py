import pytest
import uuid

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from cqrs_ddd.backends.sqlalchemy_saga import SQLAlchemySagaRepository
from cqrs_ddd.saga import SagaContext


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
            CREATE TABLE sagas (
                saga_id VARCHAR(64) PRIMARY KEY,
                saga_type VARCHAR(128),
                correlation_id VARCHAR(64),
                current_step VARCHAR(128),
                state JSON,
                history JSON,
                processed_message_ids JSON,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                completed_at TIMESTAMP,
                failed_at TIMESTAMP,
                error TEXT,
                is_completed BOOLEAN DEFAULT FALSE,
                is_failed BOOLEAN DEFAULT FALSE,
                is_stalled BOOLEAN DEFAULT FALSE,
                compensations JSON,
                failed_compensations JSON,
                pending_commands JSON,
                is_suspended BOOLEAN DEFAULT FALSE,
                suspend_reason VARCHAR(255),
                suspend_timeout_at TIMESTAMP
            )
        """
            )
        )
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


@pytest.mark.asyncio
async def test_save_and_load_insert(session_factory):
    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)
        sid = str(uuid.uuid4())
        ctx = SagaContext(
            saga_id=sid,
            saga_type="TestSaga",
            correlation_id="corr-1",
            current_step="step1",
            state={"k": "v"},
            history=[{"a": 1}],
            processed_message_ids=["m1"],
        )
        await repo.save(ctx)
        # commit to make visible to other sessions
        await session.commit()

    # load in a fresh session
    async with session_factory() as session2:
        repo2 = SQLAlchemySagaRepository(session=session2)
        loaded = await repo2.load(sid)
        assert loaded is not None
        assert loaded.saga_id == sid
        assert loaded.correlation_id == "corr-1"
        assert loaded.current_step == "step1"
        assert loaded.state == {"k": "v"}
        assert loaded.history == [{"a": 1}]
        assert loaded.processed_message_ids == ["m1"]


@pytest.mark.asyncio
async def test_save_and_update(session_factory):
    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)
        sid = "saga-update"
        ctx = SagaContext(
            saga_id=sid,
            saga_type="TestSaga",
            correlation_id="corr-2",
            current_step="stepA",
        )
        await repo.save(ctx)
        await session.commit()

    # update
    async with session_factory() as session2:
        repo2 = SQLAlchemySagaRepository(session=session2)
        loaded = await repo2.load(sid)
        assert loaded.current_step == "stepA"
        loaded.current_step = "stepB"
        loaded.state["updated"] = True
        loaded.is_completed = True
        await repo2.save(loaded)
        await session2.commit()

    async with session_factory() as session3:
        repo3 = SQLAlchemySagaRepository(session=session3)
        final = await repo3.load(sid)
        assert final.current_step == "stepB"
        assert final.state.get("updated") is True
        assert final.is_completed is True


@pytest.mark.asyncio
async def test_find_by_correlation_id(session_factory):
    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)
        sid = "saga-corr"
        ctx = SagaContext(
            saga_id=sid,
            saga_type="TestSaga",
            correlation_id="corr-xyz",
            current_step="stepX",
        )
        await repo.save(ctx)
        await session.commit()

    async with session_factory() as session2:
        repo2 = SQLAlchemySagaRepository(session=session2)
        found = await repo2.find_by_correlation_id("corr-xyz", saga_type="TestSaga")
        assert found is not None
        assert found.saga_id == sid


@pytest.mark.asyncio
async def test_timezone_conversion(engine, session_factory):
    # Insert a row with naive timestamps (no tzinfo) and ensure loaded object gets tzinfo
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
            INSERT INTO sagas (saga_id, saga_type, correlation_id, current_step, state, history, processed_message_ids, created_at, updated_at)
            VALUES (:saga_id, :saga_type, :correlation_id, :current_step, :state, :history, :processed_message_ids, :created_at, :updated_at)
        """
            ),
            {
                "saga_id": "tz-saga",
                "saga_type": "TestSaga",
                "correlation_id": "corr-tz",
                "current_step": "s",
                "state": '{"x":1}',
                "history": '[{"h":1}]',
                "processed_message_ids": '["m"]',
                "created_at": "2020-01-01 00:00:00",
                "updated_at": "2020-01-02 00:00:00",
            },
        )

    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)
        loaded = await repo.load("tz-saga")
        assert loaded is not None
        assert loaded.created_at.tzinfo is not None
        assert loaded.updated_at.tzinfo is not None
        # they should be timezone-aware and not raise when converting to isoformat
        _ = loaded.updated_at.isoformat()


@pytest.mark.asyncio
async def test_find_stalled_sagas(session_factory):
    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)

        # Create normal saga
        await repo.save(
            SagaContext(
                saga_id="1", saga_type="T", correlation_id="c1", current_step="s"
            )
        )

        # Create stalled saga
        stalled = SagaContext(
            saga_id="2", saga_type="T", correlation_id="c2", current_step="s"
        )
        stalled.is_stalled = True
        stalled.pending_commands = [{"cmd": 1}]
        await repo.save(stalled)

        await session.commit()

    async with session_factory() as session2:
        repo2 = SQLAlchemySagaRepository(session=session2)
        stalled_list = await repo2.find_stalled_sagas()
        assert len(stalled_list) == 1
        assert stalled_list[0].saga_id == "2"
        assert stalled_list[0].pending_commands == [{"cmd": 1}]


@pytest.mark.asyncio
async def test_find_by_correlation_id_with_type(session_factory):
    async with session_factory() as session:
        repo = SQLAlchemySagaRepository(session=session)
        # Two sagas with same correlation ID but different types (saga pattern standard)
        s1 = SagaContext(
            saga_id="s1",
            saga_type="TypeA",
            correlation_id="common-corr",
            current_step="s",
        )
        s2 = SagaContext(
            saga_id="s2",
            saga_type="TypeB",
            correlation_id="common-corr",
            current_step="s",
        )
        await repo.save(s1)
        await repo.save(s2)
        await session.commit()

    async with session_factory() as session2:
        repo2 = SQLAlchemySagaRepository(session=session2)

        # Find by type
        found_a = await repo2.find_by_correlation_id("common-corr", saga_type="TypeA")
        assert found_a.saga_id == "s1"

        found_b = await repo2.find_by_correlation_id("common-corr", saga_type="TypeB")
        assert found_b.saga_id == "s2"

        # Find explicit mismatch
        assert (
            await repo2.find_by_correlation_id("common-corr", saga_type="TypeC") is None
        )
