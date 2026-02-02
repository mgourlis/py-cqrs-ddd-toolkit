
import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from cqrs_ddd.saga import (
    Saga, 
    SagaContext, 
    SagaChoreographyManager, 
    InMemorySagaRepository, 
    saga_step
)
from cqrs_ddd.domain_event import DomainEventBase
from cqrs_ddd.core import Command
from cqrs_ddd.saga_registry import SagaRegistry

# --- Setup ---

@dataclass
class ApprovalRequest(DomainEventBase):
    request_id: str = ""
    amount: float = 0.0

@dataclass
class ApprovalReceived(DomainEventBase):
    request_id: str = ""
    approved: bool = True

@dataclass
class RejectRequest(Command):
    request_id: str = ""

@dataclass
class ApproveRequest(Command):
    request_id: str = ""

@dataclass
class HitlState:
    request_id: str = ""
    status: str = "init"

class ApprovalSaga(Saga[HitlState]):
    state_type = HitlState

    @saga_step(ApprovalRequest)
    async def on_request(self, event: ApprovalRequest):
        self.state.request_id = event.request_id
        if event.amount > 1000:
            self.state.status = "waiting_approval"
            # Suspend for approval with a short timeout for testing
            # Explicitly setting timeout to 1 second for test control
            self.suspend("Large amount requires manual approval", timeout=timedelta(seconds=2))
        else:
            self.state.status = "auto_approved"
            self.dispatch_command(ApproveRequest(request_id=event.request_id))
            self.complete()

    @saga_step(ApprovalReceived)
    async def on_approval(self, event: ApprovalReceived):
        # Resume implicitly happens if we process the event?
        # The base process logic loads the context. 
        # If we successfully process this step, we should clear the suspension explicitly or implicitly?
        # My implementation plan said: "resume() ... Clears is_suspended"
        # Let's call resume() explicitly to be safe and idiomatic.
        
        self.resume() 
        
        if event.approved:
            self.state.status = "approved"
            self.dispatch_command(ApproveRequest(request_id=self.state.request_id))
            self.complete()
        else:
            self.state.status = "rejected"
            self.dispatch_command(RejectRequest(request_id=self.state.request_id))
            self.complete()

    async def on_timeout(self):
        """Called when suspension times out."""
        self.state.status = "timed_out"
        # Auto-reject on timeout
        self.dispatch_command(RejectRequest(request_id=self.state.request_id))
        self.fail("Approval timed out")

# --- Tests ---

@pytest.mark.asyncio
async def test_hitl_suspend_and_resume():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    
    registry = SagaRegistry()
    registry.register(ApprovalRequest, ApprovalSaga)
    registry.register(ApprovalReceived, ApprovalSaga)
    
    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    correlation_id = "corr-hitl-1"
    
    # 1. Trigger Suspension
    evt1 = ApprovalRequest(request_id="r-1", amount=5000.0, correlation_id=correlation_id)
    await manager.handle_event(evt1)
    
    # Verify Suspended
    ctx = await repo.find_by_correlation_id(correlation_id, "ApprovalSaga")
    assert ctx is not None
    assert ctx.is_suspended
    assert ctx.suspend_reason == "Large amount requires manual approval"
    assert ctx.suspend_timeout_at is not None
    assert ctx.state['status'] == "waiting_approval"
    
    # 2. Trigger Resume (Approval)
    evt2 = ApprovalReceived(request_id="r-1", approved=True, correlation_id=correlation_id)
    await manager.handle_event(evt2)
    
    # Verify Resumed and Completed
    ctx = await repo.load(ctx.saga_id) # Reload
    assert not ctx.is_suspended
    assert ctx.is_completed
    assert ctx.state['status'] == "approved"
    
    # Check command
    assert mediator.send.called
    cmd = mediator.send.await_args[0][0]
    assert isinstance(cmd, ApproveRequest)


@pytest.mark.asyncio
async def test_hitl_timeout():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    
    registry = SagaRegistry()
    registry.register(ApprovalRequest, ApprovalSaga)
    registry.register_type(ApprovalSaga)
    
    # We need to register by NAME for process_timeouts to find the class
    # The registry usually maps Event -> Class. 
    # Does it allow looking up Class by Name? 
    # SagaRegistry.get_saga_by_name(name)?
    # Let's assume standard behavior or check registry.
    # If not, we might fail to instantiate the saga for timeout.
    # We'll check `SagaRegistry` implementation if this fails.
    # For now, ensure the class is known because we registered it for an event?
    # Or strict registry might separate name lookup. 
    # Let's hope `_find_saga_class_by_name` uses `get_saga_by_name` or iterates.
    
    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    correlation_id = "corr-hitl-timeout"
    
    # 1. Trigger Suspension (Timeout 2s)
    # We'll force a short timeout via mock logic or just wait?
    # Waiting 2s in unit test is slow. 
    # Better to manually expire the context time.
    
    evt1 = ApprovalRequest(request_id="r-2", amount=5000.0, correlation_id=correlation_id)
    await manager.handle_event(evt1)
    
    # Manually expire the timeout in DB
    ctx = await repo.find_by_correlation_id(correlation_id, "ApprovalSaga")
    ctx.suspend_timeout_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    await repo.save(ctx)
    
    # DEBUG: Check if helper finds it
    expired = await repo.find_expired_suspended_sagas()
    print(f"DEBUG: found expired sagas: {len(expired)}")
    assert len(expired) == 1
    
    # 2. Process Timeouts
    await manager.process_timeouts()
    
    # Verify Timeout Handling
    # Verify Timeout Handling
    ctx = await repo.load(ctx.saga_id)
    if not ctx.is_failed:
         pytest.fail(f"Saga should be failed but is_failed={ctx.is_failed}, suspended={ctx.is_suspended}, state={ctx.state}")
    
    assert ctx.state['status'] == "timed_out", f"Status mismatch: {ctx.state['status']}"
    assert "Approval timed out" in str(ctx.error), f"Error mismatch: {ctx.error}"
    
    # Check command (Reject)
    assert mediator.send.called
    cmd = mediator.send.await_args[0][0]
    assert isinstance(cmd, RejectRequest)

