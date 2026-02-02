import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from dataclasses import dataclass

try:
    from tenacity import RetryError
    HAS_TENACITY = True
except ImportError:
    HAS_TENACITY = False

from cqrs_ddd.saga import (
    Saga, 
    SagaChoreographyManager, 
    InMemorySagaRepository, 
    saga_step,
    SagaContext
)
from cqrs_ddd.core import Command
from cqrs_ddd.domain_event import DomainEventBase

# --- Setup ---

@dataclass
class RetryCommand(Command):
    id: str = "cmd-1"

@dataclass
class FailEvent(DomainEventBase):
    id: str = "evt-1"

@dataclass
class RetrySagaState:
    status: str = "init"

class ErrorSaga(Saga[RetrySagaState]):
    state_type = RetrySagaState

    @saga_step(FailEvent)
    async def on_fail(self, event: FailEvent):
        # This handler dispatches a command that will be mocked to fail
        self.dispatch_command(RetryCommand())
        # Or we can raise here to test step failure
        if event.id == "CRASH":
             raise ValueError("Boom!")

    async def compensate(self):
        self.state.status = "compensated"

# --- Tests ---

@pytest.mark.skipif(not HAS_TENACITY, reason="Tenacity required for retry tests")
@pytest.mark.asyncio
async def test_dispatch_retry_success():
    """Test that command dispatch retries on failure and succeeds eventually."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    # Setup mock to fail twice then succeed
    mediator.send = AsyncMock(side_effect=[ValueError("Fail 1"), ValueError("Fail 2"), None])
    
    from cqrs_ddd.saga_registry import SagaRegistry
    registry = SagaRegistry()
    registry.register(FailEvent, ErrorSaga)
    
    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    # Trigger saga
    evt = FailEvent(id="retry-test", correlation_id="corr-1")
    await manager.handle_event(evt)
    
    # Verification
    assert mediator.send.call_count == 3
    
    ctx = await repo.find_by_correlation_id("corr-1", "ErrorSaga")
    assert not ctx.is_stalled

@pytest.mark.skipif(not HAS_TENACITY, reason="Tenacity required for retry tests")
@pytest.mark.asyncio
async def test_dispatch_failure_exhaustion():
    """Test that command dispatch retries exhaust and stall the saga."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    # Always fail
    mediator.send = AsyncMock(side_effect=ValueError("Persistent Fail"))
    
    from cqrs_ddd.saga_registry import SagaRegistry
    registry = SagaRegistry()
    registry.register(FailEvent, ErrorSaga)
    
    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    # Start saga
    evt = FailEvent(id="exhaust-test", correlation_id="corr-2")
    
    # Expect error to propagate after retries
    with pytest.raises((ValueError, RetryError)):
        await manager.handle_event(evt)
    
    # Check stall state
    ctx = await repo.find_by_correlation_id("corr-2", "ErrorSaga")
    assert ctx.is_stalled
    assert "Persistent Fail" in str(ctx.error)

@pytest.mark.asyncio
async def test_saga_step_failure_compensation():
    """Test that exception in saga step triggers failure and compensation."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    
    from cqrs_ddd.saga_registry import SagaRegistry
    registry = SagaRegistry()
    registry.register(FailEvent, ErrorSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    # Trigger crash
    evt = FailEvent(id="CRASH", correlation_id="corr-3")
    
    with pytest.raises(ValueError, match="Boom!"):
        await manager.handle_event(evt)
        
    ctx = await repo.find_by_correlation_id("corr-3", "ErrorSaga")
    # Saga logic: handle_event catches, calls fail(), calls compensate(), calls save(), then re-raises
    # But fail() sets is_failed=True
    assert ctx.is_failed
    # And we implemented compensate to update state
    # Wait, assuming _sync_state called after failure logic?
    # In Saga.handle_event:
    # except: fail(), compensate(), raise
    # In SagaManager: catch except -> save context
    # Does compensate update `ctx.state`?
    # Base Saga.compensate is empty. We overrode it.
    # But usually state sync happens if successful.
    # SagaManager catch block:
    # except Exception: saga._sync_state(); await save(context); raise
    # So yes, state should be synced even on failure.
    
    # We need to manually load the state dict to check "compensated"
    # Actually context.state should reflect it.
    assert ctx.state['status'] == "compensated"
