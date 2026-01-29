
import pytest
from dataclasses import dataclass, field
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone
from typing import List, Dict, Any, Union
from cqrs_ddd.saga import (
    Saga, 
    SagaContext, 
    SagaManager, 
    saga_step, 
    InMemorySagaRepository,
    saga_registry
)
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
    pass

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

@pytest.mark.asyncio
async def test_dispatch_commands_failure_coverage():
    """Cover exception in _dispatch_commands."""
    mediator = MagicMock()
    mediator.send = AsyncMock(side_effect=Exception("Send failed"))
    
    class CompSaga(ConcreteSaga):
        compensated = False
        async def compensate(self):
            self.compensated = True
            
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    saga = CompSaga(ctx, mediator)
    saga.dispatch_command(DummyCommand())
    
    with pytest.raises(Exception, match="Send failed"):
        await saga.handle_event(HandledEvent())
        
    assert ctx.is_failed
    assert "Send failed" in ctx.error
    assert saga.compensated

@pytest.mark.asyncio
async def test_repository_correlation_id_no_type_coverage():
    """Cover find_by_correlation_id without type."""
    repo = InMemorySagaRepository()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    await repo.save(ctx)
    
    # Found
    found = await repo.find_by_correlation_id("C")
    assert found.saga_id == "1"
    
    # Not found
    assert await repo.find_by_correlation_id("MISSING") is None

@pytest.mark.asyncio
async def test_manager_handle_event_meta_correlation_coverage():
    """Cover extracting correlation_id from metadata."""
    repo = MagicMock()
    repo.find_by_correlation_id = AsyncMock(return_value=None)
    manager = SagaManager(repo, MagicMock())
    
    # Mock event with metadata
    event = MagicMock()
    del event.correlation_id # Force attribute error
    event.metadata = {"correlation_id": "meta-corr"}
    
    # We need a saga class registered
    class MetaSaga(ConcreteSaga): pass
    saga_registry.register(type(event), MetaSaga)
    
    with patch.object(manager, "_process_saga") as mock_proc:
        await manager.handle_event(event)
        mock_proc.assert_called_with(MetaSaga, event, "meta-corr")

@pytest.mark.asyncio
async def test_manager_handle_event_missing_correlation_warning(caplog):
    """Cover warning when no correlation ID found."""
    manager = SagaManager(MagicMock(), MagicMock())
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
    registry = {HandledEvent: ConcreteSaga} # Single item, not a list
    manager = SagaManager(MagicMock(), MagicMock(), saga_registry=registry)
    
    with patch.object(manager, "_process_saga") as mock_proc:
        evt = HandledEvent(correlation_id="C")
        await manager.handle_event(evt)
        mock_proc.assert_called_with(ConcreteSaga, evt, "C")

@pytest.mark.asyncio
async def test_manager_handle_event_no_sagas_return():
    """Cover early return if no sagas found."""
    manager = SagaManager(MagicMock(), MagicMock())
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
    manager = SagaManager(repo, MagicMock(), lock_strategy=lock)
    
    # Use patch to avoid registration side effects for other tests
    await manager._process_saga(ConcreteSaga, HandledEvent(), "C")
    
    lock.acquire.assert_called_with("saga", "saga:ConcreteSaga:C")
    lock.release.assert_called_with("saga", "saga:ConcreteSaga:C", "token")

@pytest.mark.asyncio
async def test_manager_process_saga_completed_early_return():
    """Cover early return for already completed/failed saga."""
    repo = MagicMock()
    ctx = SagaContext(saga_id="1", saga_type="T", correlation_id="C", current_step="S")
    ctx.is_completed = True
    repo.find_by_correlation_id = AsyncMock(return_value=ctx)
    
    manager = SagaManager(repo, MagicMock())
    
    # Use a mock class
    mock_saga_class = MagicMock()
    mock_saga_class.__name__ = "MockSaga"
    
    await manager._process_saga(mock_saga_class, HandledEvent(), "C")
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
    ctx = SagaContext(saga_id="1", saga_type="ConcreteSaga", correlation_id="C", current_step="S")
    ctx.processed_message_ids = ["evt-1"]
    await repo.save(ctx)
    
    manager = SagaManager(repo, MagicMock())
    evt = HandledEvent(event_id="evt-1")
    
    # Use a mock class
    mock_saga_class = MagicMock()
    mock_saga_class.__name__ = "ConcreteSaga"
    
    # Should return early
    await manager._process_saga(mock_saga_class, evt, "C")
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
    
    # History should contain handle_event AND dispatch_command
    assert len(ctx.history) == 2
    assert ctx.history[1]["action"] == "dispatch_command"

@pytest.mark.asyncio
async def test_manager_process_saga_save_failure_propagation():
    """Ensure repo.save(context) is called even if handling fails."""
    repo = MagicMock()
    repo.find_by_correlation_id = AsyncMock(return_value=None)
    repo.save = AsyncMock()
    
    manager = SagaManager(repo, MagicMock())
    
    class FailingSaga(ConcreteSaga):
        async def handle_event(self, e):
            raise ValueError("Inner failure")
            
    with pytest.raises(ValueError, match="Inner failure"):
        await manager._process_saga(FailingSaga, HandledEvent(), "C")
        
    # Should be called at least twice (initial create and after failure)
    assert repo.save.call_count >= 2

@pytest.mark.asyncio
async def test_saga_base_compensate_direct():
    """Cover the base compensate implementation via super()."""
    class SimpleSaga(Saga):
        async def compensate(self): 
            await super().compensate()
            
    saga = SimpleSaga(MagicMock(), MagicMock())
    await saga.compensate()
