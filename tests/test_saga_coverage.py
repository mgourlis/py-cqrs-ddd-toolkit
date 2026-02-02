import pytest
from dataclasses import dataclass, field
from unittest.mock import MagicMock, AsyncMock, patch
from cqrs_ddd.saga import (
    Saga,
    SagaContext,
    SagaChoreographyManager,
    saga_step,
    InMemorySagaRepository,
)
from cqrs_ddd.saga_registry import saga_registry
from cqrs_ddd.core import Command
from cqrs_ddd.domain_event import DomainEventBase

# --- Dummy classes ---


@dataclass
class HandledEvent(DomainEventBase):
    pass


@dataclass
class UnhandledEvent(DomainEventBase):
    pass


@dataclass
class DummyCommand(Command):
    correlation_id: str = field(default=None)


class ConcreteSaga(Saga):
    async def compensate(self):
        pass

    @saga_step(HandledEvent)
    async def on_handled(self, event: HandledEvent):
        pass


# --- Tests ---


@pytest.fixture(autouse=True)
def clean_registry():
    saga_registry.clear()
    yield
    saga_registry.clear()


def test_saga_is_active_coverage():
    """Cover is_active logic."""
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, MagicMock())

    # 1. New saga is active
    assert saga.is_active is True

    # 2. Completed is not active
    ctx.is_completed = True
    assert saga.is_active is False

    # 3. Failed is not active
    ctx.is_completed = False
    ctx.is_failed = True
    assert saga.is_active is False


@pytest.mark.asyncio
async def test_handle_event_compensation_failure_coverage(caplog):
    """Cover critical log when compensation fails."""

    class BadCompSaga(ConcreteSaga):
        async def compensate(self):
            raise ValueError("Comp error")

        @saga_step(HandledEvent)
        async def on_handled(self, event):
            raise ValueError("Flow error")

    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = BadCompSaga(ctx, MagicMock())

    import logging

    with caplog.at_level(logging.CRITICAL):
        with pytest.raises(ValueError, match="Flow error"):
            await saga.handle_event(HandledEvent())

    assert "Compensation failed for Saga 1: Comp error" in caplog.text


# @pytest.mark.asyncio
# async def test_dispatch_commands_failure_coverage():
#     """
#     Removed because dispatch happens in Manager, not in handle_event.
#     The original test assumed handle_event triggered dispatch, which is incorrect.
#     """
#     pass


@pytest.mark.asyncio
async def test_repository_correlation_id_no_type_coverage():
    """Cover find_by_correlation_id without type."""
    repo = InMemorySagaRepository()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    await repo.save(ctx)

    # Updated protocol requires saga_type
    with pytest.raises(TypeError):
        await repo.find_by_correlation_id("C")

    # Not found
    assert await repo.find_by_correlation_id("MISSING", "T") is None


@pytest.mark.asyncio
async def test_manager_handle_event_meta_correlation_coverage():
    """Cover extracting correlation_id from metadata."""
    repo = MagicMock()
    repo.find_by_correlation_id = AsyncMock(return_value=None)
    manager = SagaChoreographyManager(repo, MagicMock(), saga_registry=saga_registry)

    # Mock event with metadata
    event = MagicMock()
    del event.correlation_id  # Force attribute error
    event.metadata = {"correlation_id": "meta-corr"}

    # We need a saga class registered
    class MetaSaga(ConcreteSaga):
        pass

    saga_registry.register(type(event), MetaSaga)

    with patch.object(manager, "_process_saga") as mock_proc:
        await manager.handle_event(event)
        mock_proc.assert_called_with(MetaSaga, "meta-corr", event=event)


@pytest.mark.asyncio
async def test_manager_handle_event_missing_correlation_warning(caplog):
    """Cover warning when no correlation ID found."""
    manager = SagaChoreographyManager(
        MagicMock(), MagicMock(), saga_registry=saga_registry
    )
    event = MagicMock()
    del event.correlation_id
    event.metadata = {}

    # Need a registration to reach the correlation id check
    saga_registry.register(type(event), ConcreteSaga)

    import logging

    with caplog.at_level(logging.WARNING):
        await manager.handle_event(event)

    assert "has no correlation_id, cannot match saga" in caplog.text


@pytest.mark.asyncio
async def test_manager_handle_event_registry_override_branch():
    """Cover registry override branch and single item normalization."""
    # Mock registry protocol
    registry = MagicMock()
    registry.get_sagas_for_event.return_value = [ConcreteSaga]

    manager = SagaChoreographyManager(MagicMock(), MagicMock(), saga_registry=registry)

    with patch.object(manager, "_process_saga") as mock_proc:
        evt = HandledEvent(correlation_id="C")
        await manager.handle_event(evt)
        mock_proc.assert_called_with(ConcreteSaga, "C", event=evt)


@pytest.mark.asyncio
async def test_manager_handle_event_no_sagas_return():
    """Cover early return if no sagas found."""
    manager = SagaChoreographyManager(
        MagicMock(), MagicMock(), saga_registry=saga_registry
    )
    # No sagas registered for UnhandledEvent
    await manager.handle_event(UnhandledEvent(correlation_id="C"))
    # (Just asserting it doesn't crash)


@pytest.mark.asyncio
async def test_manager_process_saga_lock_coverage():
    """Cover lock strategy in _process_saga."""
    lock = MagicMock()
    lock.acquire = AsyncMock(return_value="token")
    lock.release = AsyncMock()

    repo = InMemorySagaRepository()
    manager = SagaChoreographyManager(
        repo, MagicMock(), lock_strategy=lock, saga_registry=saga_registry
    )

    # Use patch to avoid registration side effects for other tests
    await manager._process_saga(ConcreteSaga, "C", event=HandledEvent())

    lock.acquire.assert_called_with("saga", "saga:ConcreteSaga:C")
    lock.release.assert_called_with("saga", "saga:ConcreteSaga:C", "token")


@pytest.mark.asyncio
async def test_manager_process_saga_completed_early_return():
    """Cover early return for already completed/failed saga."""
    repo = MagicMock()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    ctx.is_completed = True
    repo.find_by_correlation_id = AsyncMock(return_value=ctx)

    manager = SagaChoreographyManager(repo, MagicMock(), saga_registry=saga_registry)

    # Use a mock class
    mock_saga_class = MagicMock()
    mock_saga_class.__name__ = "MockSaga"

    await manager._process_saga(mock_saga_class, "C", event=HandledEvent())
    # If it returns early, mock_saga_class shouldn't be instantiated
    assert not mock_saga_class.called


@pytest.mark.asyncio
async def test_saga_complete_fail_logic():
    """Cover complete and fail methods."""
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, MagicMock())

    saga.complete()
    assert ctx.is_completed is True
    assert ctx.completed_at is not None

    ctx.is_completed = False
    saga.fail("Error")
    assert ctx.is_failed is True
    assert ctx.error == "Error"


@pytest.mark.asyncio
async def test_repository_load_and_find_with_type():
    """Cover repo.load and find_by_correlation_id with type."""
    repo = InMemorySagaRepository()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    await repo.save(ctx)

    # load
    assert (await repo.load("1")).saga_id == "1"

    # find with type
    assert (await repo.find_by_correlation_id("C", saga_type="T")).saga_id == "1"
    assert await repo.find_by_correlation_id("C", saga_type="OTHER") is None


@pytest.mark.asyncio
async def test_manager_idempotency_coverage():
    """Cover idempotency check in _process_saga."""
    repo = InMemorySagaRepository()
    ctx = SagaContext(
        saga_id="1", saga_type="ConcreteSaga", correlation_id="C", current_step="S"
    )
    ctx.processed_message_ids = ["evt-1"]
    await repo.save(ctx)

    manager = SagaChoreographyManager(repo, MagicMock(), saga_registry=saga_registry)
    evt = HandledEvent(event_id="evt-1")

    # Use a mock class
    mock_saga_class = MagicMock()
    mock_saga_class.__name__ = "ConcreteSaga"

    # Should return early
    await manager._process_saga(mock_saga_class, "C", event=evt)
    assert not mock_saga_class.called


@pytest.mark.asyncio
async def test_saga_history_after_dispatch():
    """Cover history append after command dispatch."""
    mediator = MagicMock()
    mediator.send = AsyncMock()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, mediator)

    saga.dispatch_command(DummyCommand())
    await saga.handle_event(HandledEvent())

    # Verify command was queued locally
    assert len(saga._local_pending_commands) == 1
    assert isinstance(saga._local_pending_commands[0], DummyCommand)


@pytest.mark.asyncio
async def test_manager_process_saga_save_failure_propagation():
    """Ensure repo.save(context) is called even if handling fails."""
    repo = MagicMock()
    repo.find_by_correlation_id = AsyncMock(return_value=None)
    repo.save = AsyncMock()

    manager = SagaChoreographyManager(repo, MagicMock(), saga_registry=saga_registry)

    class FailingSaga(ConcreteSaga):
        async def handle_event(self, e):
            raise ValueError("Inner failure")

    with pytest.raises(ValueError, match="Inner failure"):
        await manager._process_saga(FailingSaga, "C", event=HandledEvent())

    # Should be called at least twice (initial create and after failure)
    assert repo.save.call_count >= 2


@pytest.mark.asyncio
async def test_saga_base_compensate_direct():
    """Cover the base compensate implementation via super()."""

    class SimpleSaga(Saga):
        async def compensate(self):
            await super().compensate()

    # Use real context to avoid infinite loop on mocked list boolean eval
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = SimpleSaga(ctx, MagicMock())
    await saga.compensate()


@pytest.mark.asyncio
async def test_find_stalled_sagas_coverage():
    """Cover InMemorySagaRepository.find_stalled_sagas."""
    repo = InMemorySagaRepository()
    # Create 3 stalled sagas
    for i in range(3):
        ctx = SagaContext(
            saga_id=f"stalled-{i}",
            saga_type="T",
            correlation_id=f"c-{i}",
            current_step="S",
        )
        ctx.is_stalled = True
        await repo.save(ctx)

    # Find all
    stalled = await repo.find_stalled_sagas(limit=10)
    assert len(stalled) == 3

    # Find limit
    stalled_limit = await repo.find_stalled_sagas(limit=2)
    assert len(stalled_limit) == 2


@pytest.mark.asyncio
async def test_recover_pending_sagas_coverage():
    """Cover SagaManager.recover_pending_sagas."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    manager = SagaChoreographyManager(repo, mediator, saga_registry=saga_registry)

    # Setup stalled saga with pending command
    ctx = SagaContext(
        saga_id="rec-1", saga_type="T", correlation_id="rec-c", current_step="S"
    )
    ctx.is_stalled = True

    # Manually append a serialized command
    cmd = DummyCommand(correlation_id="rec-c")
    # We need a valid serialization
    # Hack: use a dummy saga to serialize
    dummy_saga = ConcreteSaga(ctx, mediator)
    serialized = dummy_saga._serialize_command(cmd)
    ctx.pending_commands.append(serialized)

    await repo.save(ctx)

    # Run recovery
    await manager.recover_pending_sagas()

    # Assertions
    assert mediator.send.called
    updated_ctx = await repo.load("rec-1")
    assert not updated_ctx.is_stalled
    assert len(updated_ctx.pending_commands) == 0


@pytest.mark.asyncio
async def test_recover_pending_sagas_failure_coverage(caplog):
    """Cover failure during recovery (invalid command)."""
    repo = InMemorySagaRepository()
    manager = SagaChoreographyManager(repo, MagicMock(), saga_registry=saga_registry)

    ctx = SagaContext(
        saga_id="fail-rec", saga_type="T", correlation_id="c", current_step="S"
    )
    ctx.is_stalled = True
    # Add invalid command data
    ctx.pending_commands.append({"bad": "data"})
    await repo.save(ctx)

    await manager.recover_pending_sagas()

    # Should log error and remain stalled (or partial fail logic)
    # The code re-raises exception inside the loop, so it stays stalled
    updated_ctx = await repo.load("fail-rec")
    assert updated_ctx.is_stalled
    assert "Invalid serialized command" in caplog.text


@pytest.mark.asyncio
async def test_dispatch_serialized_command_coverage():
    """Cover _dispatch_serialized_command (success and error)."""
    mediator = MagicMock()
    mediator.send = AsyncMock()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, mediator)

    cmd = DummyCommand()
    serialized = saga._serialize_command(cmd)

    # Success
    await saga._dispatch_serialized_command(serialized)
    assert mediator.send.called

    # Invalid data
    with pytest.raises(ValueError, match="missing type info"):
        await saga._dispatch_serialized_command({"data": {}})

    # Import error (bad module)
    bad_cmd = serialized.copy()
    bad_cmd["module_name"] = "non.existent.module"
    with pytest.raises(ImportError):
        await saga._dispatch_serialized_command(bad_cmd)


@pytest.mark.asyncio
async def test_add_compensation_coverage():
    """Cover add_compensation."""
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, MagicMock())

    cmd = DummyCommand()
    saga.add_compensation(cmd)

    assert len(ctx.compensations) == 1
    assert ctx.compensations[0]["type_name"] == "DummyCommand"


@pytest.mark.asyncio
async def test_execute_compensations_failure_coverage(caplog):
    """Cover exception in execute_compensations loop."""
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = ConcreteSaga(ctx, MagicMock())

    # Add invalid compensation to trigger error
    ctx.compensations.append({"bad": "data"})

    await saga.execute_compensations()

    assert len(ctx.failed_compensations) == 1
    assert "Failed to execute compensation" in caplog.text


@pytest.mark.asyncio
async def test_load_state_validation_error_coverage():
    """Cover _load_state exception branches."""

    @dataclass
    class BadState:
        foo: str = ""

        # Raises error on init
        def __post_init__(self):
            raise ValueError("Init failed")

    class BadStateSaga(Saga[BadState]):
        pass

    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    ctx.state = {"foo": "bar"}  # triggers non-empty branch

    # Mocking _get_state_type via Generic inheritance works automatically in _load_state
    # because BadStateSaga(Saga[BadState]) is introspectable.
    # So __init__ calls _load_state which raises.

    with pytest.raises(ValueError, match="Saga State Corruption"):
        BadStateSaga(ctx, MagicMock())


@pytest.mark.asyncio
async def test_start_with_dataclass_coverage():
    """Cover start(initial_data) when data is dataclass."""

    @dataclass
    class MyState:
        val: int

    class MySaga(Saga[MyState]):
        pass

    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = MySaga(ctx, MagicMock())

    data = MyState(val=42)
    await saga.start(data)

    assert saga.state == data
    assert saga._state == data


@pytest.mark.asyncio
async def test_orchestrator_run_domain_event():
    """Cover SagaOrchestratorManager.run(DomainEvent)."""
    from cqrs_ddd.saga import SagaOrchestratorManager

    repo = MagicMock()
    repo.find_by_correlation_id = AsyncMock(return_value=None)
    manager = SagaOrchestratorManager(repo, MagicMock(), saga_registry=saga_registry)

    event = HandledEvent(correlation_id="C")

    with patch.object(manager, "_process_saga") as mock_proc:
        await manager.run(ConcreteSaga, event, "C")
        mock_proc.assert_called_with(ConcreteSaga, "C", event=event)


def test_missing_tenacity_coverage():
    """Cover _dispatch_with_retry when tenacity missing."""
    with patch("cqrs_ddd.saga.HAS_TENACITY", False):
        repo = InMemorySagaRepository()
        mediator = MagicMock()
        mediator.send = AsyncMock()
        manager = SagaChoreographyManager(repo, mediator, saga_registry=saga_registry)

        # Async test inside sync wrapper needs careful handling or run loop
        # But we can just call it if we are in async test...
        # Actually this test function isn't marked async.
        import asyncio

        async def run_it():
            await manager._dispatch_with_retry(DummyCommand())

        asyncio.run(run_it())
        assert mediator.send.called
