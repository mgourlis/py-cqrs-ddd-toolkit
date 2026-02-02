import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone
from typing import List

from cqrs_ddd.undo import DefaultUndoService, UndoExecutor, _EXECUTOR_CLASSES
from cqrs_ddd.domain_event import DomainEventBase
from cqrs_ddd.event_store import StoredEvent

# --- Helpers ---


@dataclass
class UndoTestEvent(DomainEventBase):
    data: str = ""

    @property
    def aggregate_id(self):
        return "agg-1"

    @property
    def aggregate_type(self):
        return "TestAggregate"


# Executor Testing
class MockUndoExecutor(UndoExecutor[UndoTestEvent]):
    async def can_undo(self, event: UndoTestEvent) -> bool:
        return event.data != "cannot_undo"

    async def undo(self, event: UndoTestEvent) -> List[DomainEventBase]:
        return [UndoTestEvent(data=f"undo_{event.data}")]

    async def redo(
        self, original: UndoTestEvent, version1: UndoTestEvent
    ) -> List[DomainEventBase]:
        return [UndoTestEvent(data=f"redo_{original.data}")]


# Helper for registration test
class AutoRegisteredExecutor(UndoExecutor[UndoTestEvent]):
    pass


# --- Tests ---


@pytest.fixture
def clean_registry():
    # Save original
    original = _EXECUTOR_CLASSES.copy()
    _EXECUTOR_CLASSES.clear()
    yield
    # Restore
    _EXECUTOR_CLASSES.clear()
    _EXECUTOR_CLASSES.update(original)


@pytest.mark.asyncio
async def test_executor_registration(clean_registry):
    # Force re-evaluation of AutoRegisteredExecutor if needed, but since it's import-time,
    # it might be tricky.

    # Actually, __init_subclass__ runs at definition time.
    # Since AutoRegisteredExecutor is defined at module level, it ran on import.
    # But clean_registry clears _EXECUTOR_CLASSES.
    # So we need to define a NEW class inside the test or trigger registration manually?
    # Or just check if MockUndoExecutor (defined at module level) is registered?

    # Let's define a new one here, but strictly to test explicit _event_type if generics fail,
    # OR rely on MockUndoExecutor being present before clear?

    # The clean_registry fixture clears the registry AFTER import.
    # So MockUndoExecutor is gone.

    # Let's define a class dynamically to trigger __init_subclass__ calls

    class DynamicExecutor(UndoExecutor[UndoTestEvent]):
        _event_type = "DynamicTestEvent"

    assert "DynamicTestEvent" in _EXECUTOR_CLASSES
    assert _EXECUTOR_CLASSES["DynamicTestEvent"] == DynamicExecutor


@pytest.mark.asyncio
async def test_get_undo_stack_filtering():
    mock_store = MagicMock()
    mock_registry = MagicMock()
    service = DefaultUndoService(mock_store, mock_registry)

    # Setup events
    # Event 1: Undone (should skip)
    # Event 2: No executor (should skip)
    # Event 3: Valid (should include)

    e1 = MagicMock(spec=StoredEvent)
    e1.is_undone = True
    e1.event_type = "E1"

    e2 = MagicMock(spec=StoredEvent)
    e2.is_undone = False
    e2.event_type = "E2"

    e3 = MagicMock(spec=StoredEvent)
    e3.is_undone = False
    e3.event_type = "UndoTestEvent"  # Matches our helper
    e3.event_id = "e3-id"
    e3.occurred_at = datetime.now(timezone.utc)
    # Mock hydration attributes usually found on StoredEvent or added during hydration
    e3.correlation_id = "corr-1"
    e3.user_id = "user-1"
    e3.causation_id = None
    e3.data = "valid"

    mock_store.get_latest_events = AsyncMock(return_value=[e1, e2, e3])

    # Registry setup
    mock_registry.has_executor.side_effect = lambda et: et == "UndoTestEvent"

    mock_executor = AsyncMock()
    mock_executor.can_undo.return_value = True
    mock_registry.get.return_value = mock_executor

    # Mock hydration
    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate") as mock_hydrate:
        mock_hydrate.side_effect = (
            lambda se: UndoTestEvent(data="valid") if se == e3 else None
        )

        stack = await service.get_undo_stack("TestAggregate", "agg-1")

        assert len(stack) == 1
        assert stack[0].event_type == "UndoTestEvent"
        assert stack[0].event_id == "e3-id"


@pytest.mark.asyncio
async def test_undo_operation_success():
    mock_store = MagicMock()
    mock_registry = MagicMock()
    service = DefaultUndoService(mock_store, mock_registry)

    # Setup stored event
    stored = MagicMock(spec=StoredEvent)
    stored.event_id = "evt-1"
    stored.event_type = "UndoTestEvent"
    stored.is_undone = False
    stored.aggregate_type = "TestAgg"
    stored.aggregate_id = "agg-1"

    mock_store.get_event = AsyncMock(return_value=stored)
    mock_store.append = AsyncMock()
    mock_store.mark_as_undone = AsyncMock()
    service._invalidate_cache = AsyncMock()

    # Executor setup
    mock_executor = AsyncMock()
    mock_executor.can_undo.return_value = True
    mock_executor.undo.return_value = [UndoTestEvent(data="compensating")]

    mock_registry.get.return_value = mock_executor

    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate") as mock_hydrate:
        mock_hydrate.return_value = UndoTestEvent(data="original")

        result = await service.undo(event_id="evt-1", user_id="user-1")

        assert result.success is True
        assert "evt-1" in result.undone_events
        assert len(result.new_events) == 1

        # Verify calls
        mock_executor.undo.assert_called_once()
        mock_store.append.assert_called_once()  # Persist compensation
        mock_store.mark_as_undone.assert_called_once_with(
            event_id="evt-1", undone_by="user-1", undo_event_id=result.new_events[0]
        )
        service._invalidate_cache.assert_awaited()


@pytest.mark.asyncio
async def test_undo_not_found():
    mock_store = MagicMock()
    service = DefaultUndoService(mock_store, MagicMock())
    mock_store.get_event = AsyncMock(return_value=None)

    result = await service.undo(event_id="missing")
    assert result.success is False
    assert "Event not found" in result.errors[0]


@pytest.mark.asyncio
async def test_redo_operation_success():
    mock_store = MagicMock()
    mock_registry = MagicMock()
    service = DefaultUndoService(mock_store, mock_registry)

    # Setup events
    # Undo event (compensating)
    undo_event = MagicMock(spec=StoredEvent)
    undo_event.event_id = "undo-1"
    undo_event.causation_id = "original-1"  # Points to original
    undo_event.correlation_id = "corr-undo"

    # Original event (undone)
    original_event = MagicMock(spec=StoredEvent)
    original_event.event_id = "original-1"
    original_event.is_undone = True
    original_event.event_type = "UndoTestEvent"
    original_event.aggregate_type = "TestAgg"
    original_event.aggregate_id = "agg-1"

    # Mock get_event calls
    async def get_event_side_effect(eid):
        if eid == "undo-1":
            return undo_event
        if eid == "original-1":
            return original_event
        return None

    mock_store.get_event = AsyncMock(side_effect=get_event_side_effect)

    mock_store.append = AsyncMock()
    mock_store.mark_as_redone = AsyncMock()
    service._invalidate_cache = AsyncMock()

    # Executor
    mock_executor = AsyncMock()
    mock_executor.redo.return_value = [UndoTestEvent(data="redo_event")]
    mock_registry.get.return_value = mock_executor

    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate") as mock_hydrate:
        mock_hydrate.side_effect = [
            UndoTestEvent(data="orig"),
            UndoTestEvent(data="undo"),
        ]

        result = await service.redo(undo_event_id="undo-1", user_id="user-1")

        assert result.success is True
        assert len(result.redone_events) == 1

        mock_store.mark_as_redone.assert_called_once_with("original-1")
        mock_store.append.assert_called_once()


@pytest.mark.asyncio
async def test_undo_executor_di_error():
    executor = MockUndoExecutor()
    with pytest.raises(RuntimeError, match="PersistenceDispatcher not injected"):
        _ = executor.persistence_dispatcher

    # Mediator is optional/None by default in current impl, checking getter
    assert executor.mediator is None


@pytest.mark.asyncio
async def test_get_undo_stack_hydration_failure():
    mock_store = MagicMock()
    service = DefaultUndoService(mock_store, MagicMock())

    event = MagicMock(spec=StoredEvent)
    event.event_type = "BadType"
    event.is_undone = False

    mock_store.get_latest_events = AsyncMock(return_value=[event])
    service.executor_registry.has_executor.return_value = True

    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate", return_value=None):
        stack = await service.get_undo_stack("Agg", "1")
        assert len(stack) == 0


@pytest.mark.asyncio
async def test_undo_by_correlation():
    mock_store = MagicMock()
    service = DefaultUndoService(mock_store, MagicMock())

    e1 = MagicMock(spec=StoredEvent)
    e1.id = 1
    e1.event_id = "e1"
    e1.is_undone = False
    e1.event_type = "UndoTestEvent"
    e1.aggregate_type = "Agg"
    e1.aggregate_id = "1"
    e2 = MagicMock(spec=StoredEvent)
    e2.id = 2
    e2.event_id = "e2"
    e2.is_undone = False
    e2.event_type = "UndoTestEvent"
    e2.aggregate_type = "Agg"
    e2.aggregate_id = "1"

    # Sorted reverse ID: e2, then e1
    mock_store.get_events_by_correlation = AsyncMock(return_value=[e1, e2])
    mock_store.append = AsyncMock()
    mock_store.mark_as_undone = AsyncMock()

    mock_cache = MagicMock()
    mock_cache.delete_batch = AsyncMock()

    service = DefaultUndoService(mock_store, MagicMock(), cache_service=mock_cache)

    # Mock executor always happy
    executor = AsyncMock()
    executor.can_undo.return_value = True
    executor.undo.return_value = [UndoTestEvent(data="comp")]
    service.executor_registry.get.return_value = executor

    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate", return_value=UndoTestEvent()):
        result = await service.undo(correlation_id="corr-1", user_id="u1")

        assert result.success is True
        assert result.undone_events == ["e2", "e1"]  # Reverse order
        assert len(result.new_events) == 2


@pytest.mark.asyncio
async def test_redo_errors():
    mock_store = MagicMock()
    service = DefaultUndoService(mock_store, MagicMock())

    # Case 1: Undo event not found
    mock_store.get_event = AsyncMock(return_value=None)
    res = await service.redo("missing")
    assert not res.success
    assert "Undo event not found" in res.errors[0]

    # Case 2: Original event not found
    undo_evt = MagicMock(spec=StoredEvent)
    undo_evt.causation_id = "orig"

    async def get_event_se(eid):
        if eid == "undo":
            return undo_evt
        return None

    mock_store.get_event = AsyncMock(side_effect=get_event_se)

    res = await service.redo(undo_event_id="undo")
    assert not res.success
    assert "Original event orig not found" in res.errors[0]
