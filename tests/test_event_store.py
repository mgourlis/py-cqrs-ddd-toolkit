import pytest
import time
from datetime import datetime
from unittest.mock import MagicMock, AsyncMock, patch
from dataclasses import dataclass, field
from typing import List

from cqrs_ddd.core import CommandResponse
from cqrs_ddd.event_store import InMemoryEventStore, StoredEvent, EventStoreMiddleware
from cqrs_ddd.domain_event import DomainEventBase
from cqrs_ddd.exceptions import CQRSDDDError

# --- Helpers ---

@dataclass
class MyEvent(DomainEventBase):
    aggregate_id: str = ""
    data: str = ""
    aggregate_type: str = "MyAggregate"
    version: int = 1

    @classmethod
    def create(cls, aggregate_id, data):
        return cls(aggregate_id=aggregate_id, data=data)

# --- InMemoryEventStore Tests ---

@pytest.mark.asyncio
async def test_append_new_stream():
    store = InMemoryEventStore()
    event = MyEvent.create("agg-1", "test")
    
    stored = await store.append(event)
    
    assert stored.id == 1
    assert stored.aggregate_version == 1
    assert len(store._events) == 1
    assert store._versions[("MyAggregate", "agg-1")] == 1

@pytest.mark.asyncio
async def test_append_concurrent_update_success():
    store = InMemoryEventStore()
    event1 = MyEvent.create("agg-1", "v1")
    await store.append(event1)
    
    event2 = MyEvent.create("agg-1", "v2")
    # Current version is 1, expected 1 (so next will be 2)
    # The store implementation logic:
    # if expected_version provided, checks current == expected.
    # new_version = current + 1
    
    stored = await store.append(event2, expected_version=1)
    
    assert stored.aggregate_version == 2
    assert store._versions[("MyAggregate", "agg-1")] == 2

@pytest.mark.asyncio
async def test_append_concurrency_error():
    store = InMemoryEventStore()
    event1 = MyEvent.create("agg-1", "v1")
    await store.append(event1)
    
    # Current version is 1.
    # Try to append stating we expect version 0 (stale write)
    event2 = MyEvent.create("agg-1", "v2")
    
    with pytest.raises(CQRSDDDError, match="Concurrency error"):
        await store.append(event2, expected_version=0)

@pytest.mark.asyncio
async def test_get_events_filtering():
    store = InMemoryEventStore()
    await store.append(MyEvent.create("agg-1", "e1"))
    await store.append(MyEvent.create("agg-1", "e2"))
    await store.append(MyEvent.create("agg-1", "e3"))
    
    # Get all
    events = await store.get_events("MyAggregate", "agg-1")
    assert len(events) == 3
    
    # From version
    events = await store.get_events("MyAggregate", "agg-1", from_version=2)
    assert len(events) == 2
    assert events[0].aggregate_version == 2
    
    # To version
    events = await store.get_events("MyAggregate", "agg-1", to_version=2)
    assert len(events) == 2
    assert events[-1].aggregate_version == 2

@pytest.mark.asyncio
async def test_undo_mechanics():
    store = InMemoryEventStore()
    stored1 = await store.append(MyEvent.create("agg-1", "e1"))
    stored2 = await store.append(MyEvent.create("agg-1", "e2"))
    
    # Mark e2 as undone
    await store.mark_as_undone(stored2.event_id, undone_by="user", undo_event_id="undo-1")
    
    # Get events should exclude e2
    events = await store.get_events("MyAggregate", "agg-1")
    assert len(events) == 1
    assert events[0].aggregate_version == 1
    
    # Get undone events
    undone = await store.get_undone_events("MyAggregate", "agg-1")
    assert len(undone) == 1
    assert undone[0].event_id == stored2.event_id
    assert undone[0].is_undone is True
    
    # Redo
    await store.mark_as_redone(stored2.event_id)
    events = await store.get_events("MyAggregate", "agg-1")
    assert len(events) == 2

@pytest.mark.asyncio
async def test_append_batch():
    store = InMemoryEventStore()
    events = [
        MyEvent.create("agg-batch", "e1"),
        MyEvent.create("agg-batch", "e2")
    ]
    
    stored_events = await store.append_batch(events, correlation_id="batch-corr-id")
    
    assert len(stored_events) == 2
    assert stored_events[0].correlation_id == "batch-corr-id"
    assert stored_events[1].correlation_id == "batch-corr-id"
    
    # Verify persistence
    fetched = await store.get_events("MyAggregate", "agg-batch")
    assert len(fetched) == 2

@pytest.mark.asyncio
async def test_get_event_by_id():
    store = InMemoryEventStore()
    stored = await store.append(MyEvent.create("agg-id", "data"))
    
    # Found
    found = await store.get_event(stored.event_id)
    assert found is not None
    assert found.event_id == stored.event_id
    
    # Not found
    assert await store.get_event("non-existent") is None

@pytest.mark.asyncio
async def test_get_events_by_correlation():
    store = InMemoryEventStore()
    e1 = MyEvent.create("agg-corr", "data1")
    e1.correlation_id = "corr-A"
    await store.append(e1)
    
    e2 = MyEvent.create("agg-corr", "data2")
    e2.correlation_id = "corr-B"
    await store.append(e2)
    
    e3 = MyEvent.create("agg-corr", "data3")
    e3.correlation_id = "corr-A"
    await store.append(e3)
    
    results = await store.get_events_by_correlation("corr-A")
    assert len(results) == 2
    assert results[0].event_id == e1.event_id
    assert results[1].event_id == e3.event_id

# --- EventStoreMiddleware Tests ---

@dataclass
class MockResult:
    events: List[MyEvent] = field(default_factory=list)

@pytest.mark.asyncio
async def test_middleware_persists_events():
    mock_store = MagicMock()
    mock_store.append_batch = AsyncMock()
    
    middleware = EventStoreMiddleware(event_store=mock_store, get_user_id=lambda: "user-123")
    
    command = MagicMock()
    
    # Handler returns events
    event = MyEvent.create("agg-1", "data")
    async def handler(*args, **kwargs):
        return CommandResponse(result="ok", events=[event])
    
    wrapped = middleware.apply(handler, command)
    await wrapped()
    
    mock_store.append_batch.assert_awaited_once()
    args, kwargs = mock_store.append_batch.call_args
    
    events = kwargs['events']
    assert len(events) == 1
    assert events[0].user_id == "user-123"
    assert events[0].correlation_id is not None # Generated
    assert kwargs['correlation_id'] == events[0].correlation_id

@pytest.mark.asyncio
async def test_middleware_ignores_no_events():
    mock_store = MagicMock()
    middleware = EventStoreMiddleware(event_store=mock_store)
    
    async def handler(*args, **kwargs):
        return CommandResponse(result="simple result")
    
    wrapped = middleware.apply(handler, MagicMock())
    await wrapped()
    
    mock_store.append_batch.assert_not_called()
