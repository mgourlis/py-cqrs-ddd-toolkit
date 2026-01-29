
import pytest
import asyncio
from typing import Any, List, Optional, Type
from datetime import datetime, timezone
import sys
import importlib
import json
import uuid
from unittest.mock import patch, MagicMock, AsyncMock

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

import cqrs_ddd.backends.sqlalchemy_event_store as event_store_mod
from cqrs_ddd.backends.sqlalchemy_event_store import SQLAlchemyEventStore
from cqrs_ddd.backends.sqlalchemy import SQLAlchemyUnitOfWork
from cqrs_ddd.exceptions import ConcurrencyError

# --- Mock Event ---

class DummyEvent:
    def __init__(self, aggregate_id, aggregate_type="dummy", version=1):
        self.event_id = str(uuid.uuid4())
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.event_type = "DummyCreated"
        self.version = version
        self.occurred_at = datetime.now(timezone.utc)
        self.user_id = "user1"
        self.correlation_id = str(uuid.uuid4())
        self.causation_id = str(uuid.uuid4())
        self.payload = {"msg": "hello"}

    def to_dict(self):
        return {
            "msg": self.payload["msg"],
            "event_id": self.event_id
        }

# --- Fixtures ---

@pytest.fixture
async def engine():
    import sqlite3
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}
    )
    async with engine.begin() as conn:
        # Create compatible SQLite table
        await conn.execute(text("""
            CREATE TABLE domain_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id VARCHAR(36) NOT NULL UNIQUE,
                aggregate_type VARCHAR(100) NOT NULL,
                aggregate_id VARCHAR(100) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                event_version INTEGER DEFAULT 1,
                occurred_at TIMESTAMP NOT NULL,
                user_id VARCHAR(255),
                correlation_id VARCHAR(36),
                causation_id VARCHAR(36),
                payload JSON NOT NULL,
                aggregate_version INTEGER NOT NULL,
                is_undone BOOLEAN DEFAULT FALSE,
                undone_at TIMESTAMP,
                undone_by VARCHAR(255),
                undo_event_id VARCHAR(36),
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
    def factory():
        return SQLAlchemyUnitOfWork(session_factory=session_factory)
    return factory

# --- Tests ---

@pytest.mark.asyncio
async def test_event_store_append_success(engine, uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    event = DummyEvent(aggregate_id="agg1")
    
    # Append without existing UoW
    result = await es.append(event)
    assert result["aggregate_version"] == 1
    assert result["aggregate_id"] == "agg1"
    
    # Verify in DB
    async with uow_factory() as uow:
        version = await es.get_current_version("dummy", "agg1", unit_of_work=uow)
        assert version == 1

@pytest.mark.asyncio
async def test_event_store_append_concurrency(uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    event1 = DummyEvent(aggregate_id="agg2")
    await es.append(event1)
    
    # Success: expected_version=1 (DB has 1, next is 2)
    event2 = DummyEvent(aggregate_id="agg2")
    await es.append(event2, expected_version=1)
    
    # Fail: expected_version=1 (DB has 2)
    event3 = DummyEvent(aggregate_id="agg2")
    with pytest.raises(ConcurrencyError):
        await es.append(event3, expected_version=1)

@pytest.mark.asyncio
async def test_event_store_batch_and_correlation(uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    events = [
        DummyEvent(aggregate_id="agg3"),
        DummyEvent(aggregate_id="agg3")
    ]
    cor_id = "target-correlation"
    
    results = await es.append_batch(events, correlation_id=cor_id)
    assert len(results) == 2
    
    # Get by correlation
    cor_events = await es.get_events_by_correlation(cor_id)
    assert len(cor_events) == 2
    assert cor_events[0]["correlation_id"] == cor_id

@pytest.mark.asyncio
async def test_event_store_retrieval_methods(uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    aid = "agg4"
    for i in range(5):
        await es.append(DummyEvent(aggregate_id=aid))
        
    # get_events
    all_events = await es.get_events("dummy", aid)
    assert len(all_events) == 5
    
    # get_events with versions
    subset = await es.get_events("dummy", aid, from_version=2, to_version=4)
    assert len(subset) == 3
    assert subset[0]["aggregate_version"] == 2
    assert subset[-1]["aggregate_version"] == 4
    
    # get_latest_events
    latest = await es.get_latest_events("dummy", aid, count=2)
    assert len(latest) == 2
    assert latest[0]["aggregate_version"] == 4
    assert latest[1]["aggregate_version"] == 5

@pytest.mark.asyncio
async def test_event_store_undo_marking(uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    aid = "agg5"
    ev = DummyEvent(aggregate_id=aid)
    await es.append(ev)
    
    await es.mark_as_undone(ev.event_id, undone_by="admin", undo_event_id="undo123")
    
    # get_events should skip undone
    events = await es.get_events("dummy", aid)
    assert len(events) == 0
    
    # latest should skip undone
    latest = await es.get_latest_events("dummy", aid)
    assert len(latest) == 0

@pytest.mark.asyncio
async def test_event_store_errors(uow_factory):
    es = SQLAlchemyEventStore(uow_factory=uow_factory)
    
    # Invalid UoW
    class BadUoW:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
    
    es_bad = SQLAlchemyEventStore(uow_factory=lambda: BadUoW())
    with pytest.raises(TypeError, match="requires a UnitOfWork with a 'session' attribute"):
        await es_bad.append(DummyEvent(aggregate_id="1"))
        
    # Append exception cleanup
    mock_uow = MagicMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock()
    mock_uow.session = MagicMock()
    mock_uow.session.execute = AsyncMock(side_effect=RuntimeError("Insert failed"))
    
    es_error = SQLAlchemyEventStore(uow_factory=lambda: mock_uow)
    with pytest.raises(RuntimeError, match="Insert failed"):
        await es_error.append(DummyEvent(aggregate_id="1"))
    
    assert mock_uow.__aexit__.called

@pytest.mark.asyncio
async def test_batch_append_exception_cleanup(uow_factory):
    mock_uow = MagicMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock()
    
    es = SQLAlchemyEventStore(uow_factory=lambda: mock_uow)
    # append_batch will fail on first append
    with patch.object(es, 'append', side_effect=RuntimeError("Batch loop fail")):
        with pytest.raises(RuntimeError, match="Batch loop fail"):
            await es.append_batch([DummyEvent(1)])
            
    assert mock_uow.__aexit__.called

@pytest.mark.asyncio
async def test_sqlalchemy_event_store_fallback():
    with patch.dict(sys.modules, {"sqlalchemy": None}):
        with patch("cqrs_ddd.backends.sqlalchemy_event_store.HAS_SQLALCHEMY", False):
            importlib.reload(event_store_mod)
            with pytest.raises(ImportError, match="SQLAlchemy is required"):
                SQLAlchemyEventStore(uow_factory=lambda: None)
    
    # Restore
    importlib.reload(event_store_mod)
