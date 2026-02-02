import pytest
from typing import Any
from unittest.mock import MagicMock, AsyncMock

try:
    from pydantic import BaseModel

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

from cqrs_ddd.saga import (
    SagaContext,
    SagaChoreographyManager,
    InMemorySagaRepository,
    saga_step,
)
from cqrs_ddd.contrib.pydantic import PydanticSaga, PydanticDomainEvent, PydanticCommand

# --- Fixtures ---

if HAS_PYDANTIC:

    class OrderState(BaseModel):
        order_id: str = ""
        status: str = "created"
        amount: float = 0.0

    class PaymentProcessed(PydanticDomainEvent):
        order_id: str = ""
        success: bool = True

    class TestOrderSaga(PydanticSaga[OrderState]):
        state_model = OrderState

        @saga_step(PaymentProcessed)
        async def on_payment(self, event: PaymentProcessed):
            self.state.status = "paid" if event.success else "failed"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_start_persistence():
    """Test that starting a PydanticSaga with a model persists state correctly."""
    repo = InMemorySagaRepository()
    # We can use ChoreographyManager and access protected method _process_saga for this test
    from cqrs_ddd.saga_registry import saga_registry

    manager = SagaChoreographyManager(repo, None, saga_registry=saga_registry)

    initial_state = OrderState(order_id="123", amount=99.9)
    correlation_id = "corr-1"

    # 1. Start (Orchestration style mocked manually)
    await manager._process_saga(
        TestOrderSaga, correlation_id, input_data=initial_state
    )

    # 2. Verify Persistence
    ctx = await repo.find_by_correlation_id(correlation_id, "TestOrderSaga")
    assert ctx is not None
    assert ctx.state["order_id"] == "123"
    assert ctx.state["amount"] == 99.9
    assert ctx.state["status"] == "created"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_event_handling():
    """Test that handling events updates Pydantic state and persists it."""
    repo = InMemorySagaRepository()
    from cqrs_ddd.saga_registry import saga_registry

    manager = SagaChoreographyManager(repo, None, saga_registry=saga_registry)

    # 1. Setup existing saga
    ctx = SagaContext(
        saga_id="saga-1",
        saga_type="TestOrderSaga",
        correlation_id="corr-2",
        current_step="started",
        state={"order_id": "123", "status": "created", "amount": 50.0},
    )
    await repo.save(ctx)

    # 2. Handle Event
    event = PaymentProcessed(order_id="123", success=True, correlation_id="corr-2")

    # Register manually to ensure manager finds it
    from cqrs_ddd.saga_registry import saga_registry

    saga_registry.register(PaymentProcessed, TestOrderSaga)

    await manager.handle_event(event)

    # 3. Verify Update
    ctx_updated = await repo.load("saga-1")
    assert ctx_updated.state["status"] == "paid"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_load_state_validation():
    """Test that loading from context validates data against Pydantic model."""
    repo = InMemorySagaRepository()

    # Corrupt state (wrong type)
    ctx = SagaContext(
        saga_id="saga-bad",
        saga_type="TestOrderSaga",
        correlation_id="corr-bad",
        current_step="started",
        state={"order_id": "123", "amount": "NOT_A_FLOAT"},
    )
    await repo.save(ctx)

    from cqrs_ddd.saga_registry import saga_registry

    manager = SagaChoreographyManager(repo, None, saga_registry=saga_registry)
    event = PaymentProcessed(order_id="123", correlation_id="corr-bad")

    from cqrs_ddd.saga_registry import saga_registry

    saga_registry.register(PaymentProcessed, TestOrderSaga)

    # Should fail during _load_state raising ValueError
    with pytest.raises(ValueError, match="Saga State Corruption"):
        await manager.handle_event(event)


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_full_orchestration():
    """Test full orchestration flow with PydanticSaga (start -> command -> event -> complete)."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()

    # We use SagaOrchestratorManager here
    from cqrs_ddd.saga import SagaOrchestratorManager

    # Define dedicated test saga for Orchestration
    class RegistrationState(BaseModel):
        email: str = ""
        user_id: str = ""

    class CreateUser(PydanticCommand):
        email: str = ""

    class UserCreated(PydanticDomainEvent):
        user_id: str = ""
        email: str = ""

    class RegistrationSaga(PydanticSaga[RegistrationState]):
        state_model = RegistrationState

        async def start(self, initial_data: Any) -> None:
            # We can init from dict or model. PydanticSaga.start handles model assignment.
            await super().start(initial_data)
            self.dispatch_command(CreateUser(email=self.state.email))

        @saga_step(UserCreated)
        async def on_user_created(self, event: UserCreated):
            self.state.user_id = event.user_id
            self.complete()

    from cqrs_ddd.saga_registry import SagaRegistry

    registry = SagaRegistry()
    registry.register(UserCreated, RegistrationSaga)

    manager = SagaOrchestratorManager(repo, mediator, saga_registry=registry)

    # 1. Start
    correlation_id = "orch-1"
    initial = RegistrationState(email="test@example.com")

    await manager.run(RegistrationSaga, initial, correlation_id)

    ctx = await repo.find_by_correlation_id(correlation_id, "RegistrationSaga")
    assert ctx.state["email"] == "test@example.com"

    # 2. Event
    # OrchestratorManager usually hands off to Choreography for events,
    # OR we use OrchestratorManager.run(event) if supported?
    # No, typically event handling is via handle_event.
    # We'll use a ChoreographyManager sharing the repo/registry for the event part
    from cqrs_ddd.saga import SagaChoreographyManager

    choreo_manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)

    event = UserCreated(
        user_id="u-1", email="test@example.com", correlation_id=correlation_id
    )
    await choreo_manager.handle_event(event)

    ctx = await repo.load(ctx.saga_id)
    assert ctx.is_completed
    assert ctx.state["user_id"] == "u-1"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_full_choreography():
    """Test full choreography flow with PydanticSaga (event -> state -> event -> complete)."""
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()

    from cqrs_ddd.saga import SagaChoreographyManager

    class FulfillmentState(BaseModel):
        order_id: str = ""
        shipped: bool = False

    class OrderPlaced(PydanticDomainEvent):
        order_id: str = ""

    class OrderShipped(PydanticDomainEvent):
        order_id: str = ""

    class FulfillmentSaga(PydanticSaga[FulfillmentState]):
        state_model = FulfillmentState

        @saga_step(OrderPlaced)
        async def on_placed(self, event: OrderPlaced):
            self.state.order_id = event.order_id

        @saga_step(OrderShipped)
        async def on_shipped(self, event: OrderShipped):
            self.state.shipped = True
            self.complete()

    from cqrs_ddd.saga_registry import SagaRegistry

    registry = SagaRegistry()
    registry.register(OrderPlaced, FulfillmentSaga)
    registry.register(OrderShipped, FulfillmentSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)

    correlation = "choreo-1"

    # 1. Start via Event
    await manager.handle_event(OrderPlaced(order_id="o-1", correlation_id=correlation))

    ctx = await repo.find_by_correlation_id(correlation, "FulfillmentSaga")
    assert ctx.state["order_id"] == "o-1"
    assert not ctx.state["shipped"]

    # 2. Continue
    await manager.handle_event(OrderShipped(order_id="o-1", correlation_id=correlation))

    ctx = await repo.load(ctx.saga_id)
    assert ctx.is_completed
    assert ctx.state["shipped"]


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_missing_state_model_fallback():
    """Test that PydanticSaga falls back to dict state if state_model is missing."""
    repo = InMemorySagaRepository()
    from cqrs_ddd.saga_registry import saga_registry

    _ = SagaChoreographyManager(repo, None, saga_registry=saga_registry)

    # subclass without state_model
    class NoModelStateSaga(PydanticSaga):
        # state_model missing
        pass

    # 1. Start (manually setup context as if started)
    ctx = SagaContext(
        saga_id="saga-nomodel",
        saga_type="NoModelStateSaga",
        correlation_id="corr-nomodel",
        current_step="init",
        state={"foo": "bar"},
    )
    await repo.save(ctx)

    # 2. Check Load behavior (via handle_event logic or manual instantiation)
    # We can manually instantiate to verify _load_state behavior
    saga = NoModelStateSaga(ctx, None)
    # _load_state is called in __init__ of Saga (base) ?
    # Let's check Saga.__init__ ...
    # Saga.__init__ calls _load_state() at the end.

    assert isinstance(saga._state, dict)
    assert saga._state["foo"] == "bar"

    # 3. Check Sync behavior
    saga._state["foo"] = "baz"
    saga._sync_state()
    assert ctx.state["foo"] == "baz"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_validation_missing_fields():
    """Test validation error when loading state with missing required fields."""
    _ = InMemorySagaRepository()

    class StrictState(BaseModel):
        required_field: str

    class StrictSaga(PydanticSaga[StrictState]):
        state_model = StrictState

    # Init with empty state (missing required_field)
    ctx = SagaContext(
        saga_id="saga-strict",
        saga_type="StrictSaga",
        correlation_id="corr-strict",
        current_step="init",
        state={"invalid": "data"},
    )

    # Instantiation should trigger _load_state -> validation error -> ValueError
    with pytest.raises(ValueError, match="Saga State Corruption"):
        StrictSaga(ctx, None)
