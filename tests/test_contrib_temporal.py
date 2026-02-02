
import pytest
from unittest.mock import MagicMock, AsyncMock
from dataclasses import dataclass

from cqrs_ddd.domain_event import DomainEventBase
from cqrs_ddd.core import Command
from cqrs_ddd.saga import Saga
from cqrs_ddd.contrib.temporal import (
    TemporalOrchestrator,
    TemporalSagaWrapper,
    TemporalSagaOrchestratorManager,
    TemporalSagaChoreographyManager,
    TemporalCommandHandler,
    HAS_TEMPORAL
)
from cqrs_ddd.saga_registry import saga_registry

# Mock temporalio if not available
if not HAS_TEMPORAL:
    pytest.skip("skipping temporal tests because temporalio is not installed", allow_module_level=True)

@dataclass
class OrderCreated(DomainEventBase):
    order_id: str = "o-123"
    
    @property
    def aggregate_id(self):
        return self.order_id
        
    def to_dict(self):
        data = super().to_dict()
        data['order_id'] = self.order_id
        return data

@dataclass
class CreateRepo(Command):
    repo_name: str = "my-repo"

class OrderSaga(Saga):
    pass

@pytest.mark.asyncio
async def test_temporal_choreography_manager():
    client = MagicMock()
    client.signal_with_start_workflow = AsyncMock()
    
    # Setup Manager
    registry = MagicMock()
    registry.get_sagas_for_event.return_value = [OrderSaga]
    
    manager = TemporalSagaChoreographyManager(
        client=client,
        task_queue="test-queue",
        saga_registry=registry
    )
    
    # Handle Event
    event = OrderCreated(order_id="o-123", correlation_id="c-999")
    await manager.handle_event(event)
    
    # Verify Signal
    client.signal_with_start_workflow.assert_called_once()
    call_kwargs = client.signal_with_start_workflow.call_args.kwargs
    
    assert call_kwargs['workflow'] == "OrderSaga"
    assert call_kwargs['workflow_id'] == "OrderSaga-c-999"
    assert call_kwargs['task_queue'] == "test-queue"
    assert call_kwargs['signal'] == "OrderCreated"
    assert call_kwargs['signal_args'][0]['order_id'] == "o-123"


@pytest.mark.asyncio
async def test_temporal_orchestrator_manager():
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=MagicMock(id="run-1"))
    
    manager = TemporalSagaOrchestratorManager(client, task_queue="q2", saga_registry=saga_registry)
    
    # Run Orchestration
    initial_data = {"foo": "bar"}
    run_id = await manager.run(OrderSaga, initial_data, correlation_id="c-888")
    
    # Verify Start
    client.start_workflow.assert_called_once()
    call_kwargs = client.start_workflow.call_args.kwargs
    
    assert call_kwargs['workflow'] == "OrderSaga"
    assert call_kwargs['id'] == "OrderSaga-c-888"
    assert call_kwargs['task_queue'] == "q2"
    assert call_kwargs['args'] == [{"foo": "bar"}]
    assert run_id == "run-1"


@pytest.mark.asyncio
async def test_temporal_command_handler():
    client = MagicMock()
    client.start_workflow = AsyncMock(return_value=MagicMock(id="wf-cmd-1"))
    
    class CreateRepoHandler(TemporalCommandHandler):
        workflow_name = "RepoWorkflow"
        task_queue = "q-cmd"
    
    handler = CreateRepoHandler(client)
    cmd = CreateRepo(repo_name="test-repo")
    cmd.correlation_id = "c-777"
    
    # Handle Command
    response = await handler.handle(cmd)
    
    # Verify Start
    client.start_workflow.assert_called_once()
    args, kwargs = client.start_workflow.call_args
    # args passed positionally in handler: client.start_workflow(name, args=..., id=..., task_queue=...)
    # Updated handler implementation uses positional args matching signature or kwargs?
    # Our implementation: await self.client.start_workflow(self.workflow_name, args=args, id=..., task_queue=...)
    
    name_arg = args[0]
    assert name_arg == "RepoWorkflow"
    
    kw = kwargs
    assert kw['id'] == "c-777"
    assert kw['task_queue'] == "q-cmd"
    assert kw['args'][0]['repo_name'] == "test-repo"
    assert response.result == "wf-cmd-1"

