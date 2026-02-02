import pytest
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass
from typing import Any

from cqrs_ddd.saga import (
    Saga, 
    SagaContext, 
    SagaOrchestratorManager, 
    InMemorySagaRepository,
    saga_step
)
from cqrs_ddd.core import Command
from cqrs_ddd.domain_event import DomainEventBase

# --- Setup ---

@dataclass
class CreateUser(Command):
    username: str = ""
    email: str = ""

@dataclass
class UserCreated(DomainEventBase):
    user_id: str = ""
    username: str = ""

@dataclass
class SendWelcomeEmail(Command):
    user_id: str = ""

@dataclass
class RegistrationState:
    username: str = ""
    user_id: str = ""
    step: str = "init"

class UserRegistrationSaga(Saga[RegistrationState]):
    state_type = RegistrationState

    async def start(self, initial_data: Any) -> None:
        self.state.username = initial_data['username']
        self.state.step = "started"
        # Dispatch command
        self.dispatch_command(CreateUser(username=self.state.username, email=initial_data['email']))

    @saga_step(UserCreated)
    async def on_user_created(self, event: UserCreated):
        self.state.user_id = event.user_id
        self.state.step = "user_created"
        self.dispatch_command(SendWelcomeEmail(user_id=event.user_id))
        self.complete()

# --- Tests ---

@pytest.mark.asyncio
async def test_orchestration_flow():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    
    # Register purely for event handling later, but start() is explicit
    from cqrs_ddd.saga_registry import SagaRegistry
    registry = SagaRegistry()
    registry.register(UserCreated, UserRegistrationSaga)

    manager = SagaOrchestratorManager(repo, mediator, saga_registry=registry)
    
    # 1. Start Orchestration
    correlation_id = "corr-orch-1"
    initial_data = {"username": "john_doe", "email": "john@example.com"}
    
    saga_id = await manager.run(UserRegistrationSaga, initial_data, correlation_id)
    
    # Verify initial state and command dispatch
    ctx = await repo.find_by_correlation_id(correlation_id, "UserRegistrationSaga")
    assert ctx is not None
    assert ctx.state['username'] == "john_doe"
    assert ctx.state['step'] == "started"
    
    assert mediator.send.called
    cmd1 = mediator.send.await_args[0][0]
    assert isinstance(cmd1, CreateUser)
    assert cmd1.username == "john_doe"
    
    # 2. Continue with Event (Choreography step within Orchestration)
    # The OrchestratorManager can also handle events if it inherits/uses _process_saga logic correctly
    # Note: SagaOrchestratorManager inherits from SagaManager, but typically handle_event is on SagaChoreographyManager.
    # However, nothing stops us from using handle_event logic if we added it or used a hybrid manager.
    # IN FACT: SagaOrchestratorManager implementation in saga.py DOES NOT implement handle_event!
    # It only has run().
    # So strictly speaking, a pure SagaOrchestratorManager might not listen to events?
    # Actually, usually an Orchestrator launches a saga, and then that saga waits for events.
    # So avoiding `handle_event` on the manager means we can't progress?
    # Let's check saga.py again. SagaOrchestratorManager ONLY defines run().
    # It seems we need a manager that can do BOTH if we want to continue via events.
    # Or we use SagaChoreographyManager to handle the events for the saga started by Orchestrator.
    # Let's test using a Choreography manager for the second step.
    
    choreography_manager = manager # If we want to simulate unified handling, usually we'd have one manager instance.
    # But since classes are split...
    from cqrs_ddd.saga import SagaChoreographyManager
    event_manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    event = UserCreated(user_id="u-123", username="john_doe", correlation_id=correlation_id)
    await event_manager.handle_event(event)
    
    # Verify completion
    ctx2 = await repo.load(saga_id)
    assert ctx2.is_completed
    assert ctx2.state['user_id'] == "u-123"
    
    # Verify second command
    last_call = mediator.send.call_args[0][0] # mock call_args is most recent
    assert isinstance(last_call, SendWelcomeEmail)
    assert last_call.user_id == "u-123"
