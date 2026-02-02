import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock

from cqrs_ddd.saga import (
    Saga,
    SagaChoreographyManager,
    saga_step,
    InMemorySagaRepository,
)
from cqrs_ddd.core import Command
from cqrs_ddd.domain_event import (
    DomainEventBase,
    generate_event_id,
    generate_correlation_id,
)
from cqrs_ddd.saga_registry import saga_registry

# --- Helpers ---


@dataclass
class OrderCreated(DomainEventBase):
    order_id: str = ""
    amount: float = 0.0


@dataclass
class PaymentProcessed(DomainEventBase):
    order_id: str = ""
    status: str = "success"


@dataclass
class ShipOrder(Command):
    order_id: str = ""


@dataclass
class CancelOrder(Command):
    order_id: str = ""


# Define Saga
class OrderSaga(Saga):
    compensated = False

    @saga_step(OrderCreated)
    async def on_order_created(self, event: OrderCreated):
        self.context.state["order_id"] = event.order_id
        self.context.state["amount"] = event.amount
        self.context.current_step = "order_created"

    @saga_step(PaymentProcessed)
    async def on_payment_processed(self, event: PaymentProcessed):
        if event.status == "fail":
            raise ValueError("Payment failed")

        self.context.state["payment_status"] = event.status
        self.context.current_step = "payment_processed"

        # Dispatch command
        self.dispatch_command(ShipOrder(order_id=self.context.state["order_id"]))
        self.complete()

    async def compensate(self):
        self.compensated = True
        self.dispatch_command(
            CancelOrder(order_id=self.context.state.get("order_id", "unknown"))
        )


# --- Tests ---


@pytest.fixture
def clean_saga_registry():
    saga_registry.clear()
    yield
    saga_registry.clear()


@pytest.mark.asyncio
async def test_saga_registration(clean_saga_registry):
    # Triggers registration? No, strict DI removed auto-registration.
    # We must register manually.
    class TestSaga(Saga):
        @saga_step(OrderCreated)
        async def step1(self, event):
            pass

        async def compensate(self):
            pass

    saga_registry.register(OrderCreated, TestSaga)

    sagas = saga_registry.get_sagas_for_event(OrderCreated)
    assert TestSaga in sagas
    assert len(sagas) == 1


@pytest.mark.asyncio
async def test_saga_flow_success():
    # Setup
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()

    # We need to manually register OrderSaga if clean_saga_registry cleared it,
    # OR we rely on import time registration effectively.
    # To be safe, let's inject registry explicitly or re-register
    # Inject registry explicitly
    from cqrs_ddd.saga_registry import SagaRegistry

    registry = SagaRegistry()
    registry.register(OrderCreated, OrderSaga)
    registry.register(PaymentProcessed, OrderSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)

    # 1. Start Saga
    correlation_id = generate_correlation_id()
    evt1 = OrderCreated(
        event_id=generate_event_id(),
        correlation_id=correlation_id,
        order_id="order-1",
        amount=100.0,
    )

    await manager.handle_event(evt1)

    # Verify state
    ctx = await repo.find_by_correlation_id(correlation_id, "OrderSaga")
    assert ctx is not None
    assert ctx.state["order_id"] == "order-1"
    assert ctx.current_step == "order_created"
    assert not ctx.is_completed

    # 2. Continue Saga
    evt2 = PaymentProcessed(
        event_id=generate_event_id(), correlation_id=correlation_id, order_id="order-1"
    )

    await manager.handle_event(evt2)

    # Verify completion
    ctx = await repo.load(ctx.saga_id)  # Reload
    assert ctx.is_completed
    assert ctx.state["payment_status"] == "success"

    # Verify command dispatched
    assert mediator.send.await_count == 1
    call_args = mediator.send.await_args[0][0]
    assert isinstance(call_args, ShipOrder)
    assert call_args.order_id == "order-1"


@pytest.mark.asyncio
async def test_saga_idempotency():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()

    from cqrs_ddd.saga_registry import SagaRegistry

    registry = SagaRegistry()
    registry.register(OrderCreated, OrderSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)

    correlation_id = "corr-idempotent"
    evt = OrderCreated(event_id="evt-1", correlation_id=correlation_id, order_id="o1")

    # First run
    await manager.handle_event(evt)
    ctx_1 = await repo.find_by_correlation_id(correlation_id, "OrderSaga")
    assert len(ctx_1.processed_message_ids) == 1

    # Second run (same event ID)
    await manager.handle_event(evt)
    ctx_2 = await repo.find_by_correlation_id(correlation_id, "OrderSaga")

    # State should not change (impl detail: processed_message_ids count same?
    # Actually logic simply returns if in list, so list shouldn't grow)
    assert len(ctx_2.processed_message_ids) == 1
    # Check history to be sure handler wasn't called twice?
    # History is appended in handle_event. If returned early, no history append.
    assert len(ctx_2.history) == 1  # Only one handling


@pytest.mark.asyncio
async def test_saga_failure_compensation():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()

    from cqrs_ddd.saga_registry import SagaRegistry

    registry = SagaRegistry()
    registry.register(OrderCreated, OrderSaga)
    registry.register(PaymentProcessed, OrderSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)

    correlation_id = "corr-fail"

    # Start it
    await manager.handle_event(
        OrderCreated(event_id="e1", correlation_id=correlation_id, order_id="o1")
    )

    # Trigger failure
    fail_evt = PaymentProcessed(
        event_id="e2", correlation_id=correlation_id, status="fail"
    )

    # Expect failure to propagate? SagaManager re-raises?
    # Logic: handle_event catches? No. SagaManager._process_saga catches and logs, then re-raises.

    with pytest.raises(ValueError, match="Payment failed"):
        await manager.handle_event(fail_evt)

    ctx = await repo.find_by_correlation_id(correlation_id, "OrderSaga")
    assert ctx.is_failed
    assert "Payment failed" in ctx.error

    # Verify compensation
    # We can check if CancelOrder command was dispatched?
    # Or check side effect on saga instance?
    # Since we can't easily access the saga instance created inside manager,
    # checking the dispatched command is best.

    assert not mediator.send.called

    # But commands should be captured in pending_commands
    assert len(ctx.pending_commands) > 0
    # Last one should be CancelOrder
    # (Checking serialized format)
    # serialized command: {"command_id":..., "correlation_id":..., "order_id":..., "__type__": "CancelOrder"}
    # Implementation detail of _serialize_command?
    # Usually it's just keys. The type isn't always embedded unless handled.
    # Base saga just does asdict.

    last_cmd = ctx.pending_commands[-1]
    # _serialize_command structure: {'type_name':..., 'data': {...}}
    assert last_cmd["data"]["order_id"] == "o1"
