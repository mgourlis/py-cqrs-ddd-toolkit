import pytest
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass

from cqrs_ddd.saga import (
    Saga, 
    SagaChoreographyManager, 
    InMemorySagaRepository,
    saga_step
)
from cqrs_ddd.core import Command
from cqrs_ddd.domain_event import DomainEventBase

# --- Setup ---

@dataclass
class OrderReceived(DomainEventBase):
    order_id: str = ""
    amount: float = 0.0

@dataclass
class InventoryReserved(DomainEventBase):
    order_id: str = ""
    success: bool = True

@dataclass
class ShipOrder(Command):
    order_id: str = ""

@dataclass
class ChoreographyState:
    order_id: str = ""
    status: str = "init"

class OrderFulfillmentSaga(Saga[ChoreographyState]):
    state_type = ChoreographyState

    @saga_step(OrderReceived)
    async def on_order_received(self, event: OrderReceived):
        self.state.order_id = event.order_id
        self.state.status = "inventory_pending"
        # No command dispatch here for simplicity, just state change waiting for next event

    @saga_step(InventoryReserved)
    async def on_inventory_reserved(self, event: InventoryReserved):
        if event.success and self.state.status == "inventory_pending":
            self.state.status = "ready_to_ship"
            self.dispatch_command(ShipOrder(order_id=self.context.state['order_id']))
            self.complete()

# --- Tests ---

@pytest.mark.asyncio
async def test_choreography_flow():
    repo = InMemorySagaRepository()
    mediator = MagicMock()
    mediator.send = AsyncMock()
    
    # Auto-matching via registry
    # Auto-matching via registry
    from cqrs_ddd.saga_registry import SagaRegistry
    registry = SagaRegistry()
    registry.register(OrderReceived, OrderFulfillmentSaga)
    registry.register(InventoryReserved, OrderFulfillmentSaga)

    manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
    
    correlation_id = "corr-choreo-1"
    
    # 1. First Event (Triggers creation)
    evt1 = OrderReceived(order_id="o-555", amount=120.0, correlation_id=correlation_id)
    await manager.handle_event(evt1)
    
    # Verify state
    ctx = await repo.find_by_correlation_id(correlation_id, "OrderFulfillmentSaga")
    assert ctx is not None
    assert ctx.state['order_id'] == "o-555"
    assert ctx.state['status'] == "inventory_pending"
    
    # 2. Second Event (Triggers continuation)
    evt2 = InventoryReserved(order_id="o-555", success=True, correlation_id=correlation_id)
    await manager.handle_event(evt2)
    
    # Verify completion
    ctx2 = await repo.load(ctx.saga_id)
    assert ctx2.is_completed
    assert ctx2.state['status'] == "ready_to_ship"
    
    assert mediator.send.called
    cmd = mediator.send.await_args[0][0]
    assert isinstance(cmd, ShipOrder)
    assert cmd.order_id == "o-555"
