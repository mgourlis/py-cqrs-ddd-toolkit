import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from cqrs_ddd.undo import (
    DefaultUndoService,
    UndoExecutor,
    DefaultUndoExecutorRegistry,
    _EXECUTOR_CLASSES,
)
from cqrs_ddd.domain_event import DomainEventBase

# --- Helpers ---


class CoverageTestEvent(DomainEventBase):
    pass


class SimpleExecutor(UndoExecutor[CoverageTestEvent]):
    async def undo(self, event):
        return []

    async def redo(self, original, undo):
        return []


# --- Tests ---


@pytest.mark.asyncio
async def test_registry_internals():
    """Cover registry introspection and fallbacks."""
    # 1. Test registration failure fallback (Name convention)
    # We define a class that doesn't inherit Generic properly to fail introspection
    # but has the name convention.

    # We need to patch typing.get_origin to raise an exception to hit line 148
    with patch("typing.get_origin", side_effect=Exception("Introspection fail")):
        # Defining class triggers __init_subclass__
        class IntrospectionFailUndoExecutor(UndoExecutor):
            pass

        # It should still register via name convention "IntrospectionFail"
        assert "IntrospectionFail" in _EXECUTOR_CLASSES

    # 2. Test explicit _event_type
    class ExplicitTypeUndoExecutor(UndoExecutor):
        _event_type = "ExplicitType"

    assert "ExplicitType" in _EXECUTOR_CLASSES

    # 3. Test Registry class methods
    registry = DefaultUndoExecutorRegistry()
    registry.register(None)  # Should pass (empty method)
    assert registry.has_executor("ExplicitType")
    assert registry.get("ExplicitType") is not None
    assert registry.get("MissingType") is None


@pytest.mark.asyncio
async def test_undo_executor_property_crash():
    """Cover RuntimeError when deps are missing."""
    ex = SimpleExecutor()
    with pytest.raises(RuntimeError):
        _ = ex.persistence_dispatcher


@pytest.mark.asyncio
async def test_get_undo_stack_edge_cases():
    """Cover depth limit and exceptions in loop."""
    store = MagicMock()
    registry = MagicMock()
    service = DefaultUndoService(store, registry)

    e1 = MagicMock()
    e1.event_type = "T1"
    e1.is_undone = False
    e1.id = 1
    e2 = MagicMock()
    e2.event_type = "T2"
    e2.is_undone = False
    e2.id = 2

    store.get_latest_events = AsyncMock(return_value=[e1, e2])
    registry.has_executor.return_value = True

    # Mock executor that raises exception on can_undo
    bad_executor = AsyncMock()
    bad_executor.can_undo.side_effect = Exception("Boom")
    registry.get.return_value = bad_executor

    with patch(
        "cqrs_ddd.undo.EventTypeRegistry.hydrate", return_value=CoverageTestEvent()
    ):
        # This covers lines 272-274 (exception in can_undo)
        stack = await service.get_undo_stack("Agg", "1", depth=1)
        # Should be empty because can_undo failed -> treated as False
        assert len(stack) == 0


@pytest.mark.asyncio
async def test_get_undo_stack_depth_break():
    """Cover 'break' when depth reached."""
    store = MagicMock()
    registry = MagicMock()
    service = DefaultUndoService(store, registry)

    # Return 2 valid events
    e1 = MagicMock()
    e1.event_type = "T1"
    e1.is_undone = False
    e1.event_id = "e1"
    e1.correlation_id = "c1"
    e1.id = 1
    e2 = MagicMock()
    e2.event_type = "T1"
    e2.is_undone = False
    e2.event_id = "e2"
    e2.correlation_id = "c2"
    e2.id = 2

    store.get_latest_events = AsyncMock(return_value=[e2, e1])  # Reverse order in logic
    registry.has_executor.return_value = True

    executor = AsyncMock()
    executor.can_undo.return_value = True
    registry.get.return_value = executor

    with patch(
        "cqrs_ddd.undo.EventTypeRegistry.hydrate", return_value=CoverageTestEvent()
    ):
        # Request depth 1, provide 2 events
        # Loop should process e1, add it, hit depth=1, and break
        stack = await service.get_undo_stack("Agg", "1", depth=1)
        assert len(stack) == 1
        assert stack[0].event_id == "e1"


@pytest.mark.asyncio
async def test_undo_loop_branches():
    """Cover continue statements and exceptions in undo loop."""
    store = MagicMock()
    registry = MagicMock()
    cache = MagicMock()
    service = DefaultUndoService(store, registry, cache)

    # Setup mixed bag of events with IDs for sorting
    e_undone = MagicMock()
    e_undone.is_undone = True
    e_undone.id = 1  # Hit 364

    e_no_exec = MagicMock()
    e_no_exec.is_undone = False
    e_no_exec.event_type = "NoExec"
    e_no_exec.id = 2

    e_bad_hydrate = MagicMock()
    e_bad_hydrate.is_undone = False
    e_bad_hydrate.event_type = "BadH"
    e_bad_hydrate.id = 3

    e_cant_undo = MagicMock()
    e_cant_undo.is_undone = False
    e_cant_undo.event_type = "Cant"
    e_cant_undo.id = 4
    e_cant_undo.event_id = "cant-id"

    e_crash = MagicMock()
    e_crash.is_undone = False
    e_crash.event_type = "Crash"
    e_crash.id = 5
    e_crash.event_id = "crash-id"

    # Setup get_event return
    async def get_by_corr(cid):
        return [e_undone, e_no_exec, e_bad_hydrate, e_cant_undo, e_crash]

    store.get_events_by_correlation = get_by_corr

    # Registry logic
    def get_executor(etype):
        if etype == "NoExec":
            return None  # Hit 369
        ex = AsyncMock()
        if etype == "Cant":
            ex.can_undo.return_value = False  # Hit 382
        if etype == "Crash":
            ex.can_undo.side_effect = Exception("UndoFail")  # Hit 420
        return ex

    registry.get.side_effect = get_executor

    # Hydration logic
    def hydrate(evt):
        if evt.event_type == "BadH":
            return None  # Hit 375
        return CoverageTestEvent()

    with patch("cqrs_ddd.undo.EventTypeRegistry.hydrate", side_effect=hydrate):
        cache.delete_batch.side_effect = Exception("CacheFail")  # Hit 457

        result = await service.undo(correlation_id="TestMix")

        # All attempted events should fail for different reasons
        assert result.success is False
        assert len(result.errors) > 0
        # Check specific error messages exist
        err_str = str(result.errors)
        assert "No undo executor" in err_str
        assert "Cannot hydrate" in err_str
        assert "business rule violation" in err_str
        assert "Error undoing crash-id" in err_str


@pytest.mark.asyncio
async def test_undo_early_returns():
    """Cover validation early returns."""
    service = DefaultUndoService(MagicMock(), MagicMock())

    # Missing args (Hit 315)
    res = await service.undo()
    assert "Must provide" in res.errors[0]

    # No events found (Hit 347)
    service.event_store.get_events_by_correlation = AsyncMock(return_value=[])
    res = await service.undo(correlation_id="empty")
    assert "No events found" in res.errors[0]


@pytest.mark.asyncio
async def test_redo_branches():
    """Cover redo loop continue/error paths."""
    store = MagicMock()
    registry = MagicMock()
    service = DefaultUndoService(store, registry)

    # Undo event (base list)
    u_bad_orig = MagicMock()
    u_bad_orig.causation_id = None
    u_bad_orig.id = 1  # Hit 523
    u_miss_orig = MagicMock()
    u_miss_orig.causation_id = "miss_id"
    u_miss_orig.id = 2
    u_ok = MagicMock()
    u_ok.event_id = "u_ok"
    u_ok.causation_id = "ok_id"
    u_ok.id = 3

    store.get_events_by_correlation = AsyncMock(
        return_value=[u_bad_orig, u_miss_orig, u_ok]
    )

    # Orig events
    e_ok = MagicMock()
    e_ok.is_undone = True
    e_ok.event_type = "Ok"
    e_ok.aggregate_type = "A"
    e_ok.aggregate_id = "1"

    async def get_event(eid):
        if eid == "miss_id":
            return None  # Hit 528
        if eid == "ok_id":
            return e_ok
        return None

    store.get_event = AsyncMock(side_effect=get_event)

    # Registry
    ex_crash = AsyncMock()
    ex_crash.redo.side_effect = Exception("RedoCrash")  # Hit 575
    registry.get.return_value = ex_crash

    with patch(
        "cqrs_ddd.undo.EventTypeRegistry.hydrate", return_value=CoverageTestEvent()
    ):
        result = await service.redo(correlation_id="TestRedo")

        assert result.success is False
        assert "Cannot determine original" in str(result.errors)
        assert "not found" in str(result.errors)
        assert "Error during redo" in str(result.errors)


@pytest.mark.asyncio
async def test_redo_sorting_and_missing():
    """Cover redo correlation sorting and empty list."""
    store = MagicMock()
    service = DefaultUndoService(store, MagicMock())

    # Hit 506 (No events)
    store.get_events_by_correlation = AsyncMock(return_value=[])
    res = await service.redo(correlation_id="empty")
    assert "No undo events" in res.errors[0]


@pytest.mark.asyncio
async def test_cache_invalidation_skip():
    """Cover early return in invalidate_cache."""
    service = DefaultUndoService(MagicMock(), MagicMock(), cache_service=None)
    # Should just return without error (Hit 447)
    await service._invalidate_cache("A", "1")
