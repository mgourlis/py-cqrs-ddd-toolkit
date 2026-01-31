"""
Temporal integration for CQRS/DDD toolkit.

Allows using Temporal Workflows as Process Managers/Sagas.
Connects Temporal to the MediatR-style dispatching of this toolkit.
"""
import logging
from typing import Any, Dict, Optional, Type, List, Union
import dataclasses
import json

from ..core import Command, CommandHandler, CommandResponse, DomainEvent
from ..mediator import Mediator

logger = logging.getLogger("cqrs_ddd.contrib.temporal")

try:
    from temporalio import workflow, activity
    from temporalio.client import Client
    from temporalio.common import RetryPolicy
    HAS_TEMPORAL = True
except ImportError:
    HAS_TEMPORAL = False
    # Mock classes to prevent ImportErrors when not using Temporal
    Client = Any
    
    class MockActivity:
        def defn(self, *args, **kwargs):
            return lambda x: x
    activity = MockActivity()


class CQRSActivity:
    """
    Generic Temporal Activity for dispatching CQRS Commands.
    
    Usage in Workflow:
        @workflow.defn
        class MySaga:
            @workflow.run
            async def run(self, ...):
                # Execute a command via the activity
                await workflow.execute_activity(
                    CQRSActivity.dispatch_command,
                    args=[{"type": "CreateUser", "data": {...}}],
                    start_to_close_timeout=timedelta(minutes=1)
                )
    """

    def __init__(self, mediator: Mediator, command_registry: Dict[str, Type[Command]] = None):
        """
        Initialize with a mediator and optional command registry.
        
        If command_registry is None, it will try to resolve command class 
        by name dynamically (which requires the command class to be imported/available).
        """
        self.mediator = mediator
        self.command_registry = command_registry or {}

    @activity.defn(name="dispatch_command")
    async def dispatch_command(self, command_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Activity to dispatch a command via the Mediator.
        
        Args:
            command_payload: Dict with 'type' and 'data' keys.
                - type: Name of the command class (e.g., "CreateUser")
                - data: Dictionary of command arguments
        
        Returns:
            The result of the command execution (serialized).
        """
        cmd_type_name = command_payload.get("type")
        cmd_data = command_payload.get("data", {})
        
        if not cmd_type_name:
            raise ValueError("Command payload must contain 'type'")

        # Resolve command class
        cmd_class = self._resolve_command_class(cmd_type_name)
        
        try:
            # Instantiate command
            # We assume kw_only=True dataclasses for Commands
            command = cmd_class(**cmd_data)
            
            # Dispatch
            # Note: Mediator.send is async
            response = await self.mediator.send(command)
            
            # Return result (must be serializable)
            # Response is typically CommandResponse object
            if hasattr(response, "to_dict"):
                return response.to_dict()
            elif hasattr(response, "result"):
                 # Handle generic CommandResponse
                 return self._serialize(response.result)
            else:
                return self._serialize(response)
                
        except Exception as e:
            logger.error(f"Failed to dispatch command {cmd_type_name}: {e}")
            raise

    def _resolve_command_class(self, type_name: str) -> Type[Command]:
        """Resolve command class from registry or global lookup."""
        # 1. Check registry
        if type_name in self.command_registry:
            return self.command_registry[type_name]
            
        # 2. Try to find in standard locations or memory
        # This is a naive implementation; in production, strict registry is safer
        for cls in Command.__subclasses__():
            if cls.__name__ == type_name:
                return cls
                
        raise ValueError(f"Command class '{type_name}' not found. Please register it.")

    def _serialize(self, obj: Any) -> Any:
        """Helper to ensure result is JSON-serializable."""
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        # Add timestamp handling etc if needed
        return obj


class TemporalCommandHandler(CommandHandler[str]):
    """
    Base Command Handler that starts a Temporal Workflow.
    
    Inherit from this class and define `workflow_name` and `task_queue`.
    The `handle` method will automatically start the workflow and return the Workflow ID.
    
    Usage:
        class DeleteRepoHandler(TemporalCommandHandler):
            workflow_name = "DeleteRepoWorkflow"
            task_queue = "main-queue"
    """
    
    workflow_name: str
    task_queue: str
    
    def __init__(self, client: Client):
        self.client = client
        
    async def handle(self, command: Command) -> CommandResponse[str]:
        if not HAS_TEMPORAL:
            raise ImportError("temporalio is required")
            
        # Determine Workflow ID (default to command.correlation_id or command_id)
        # Using correlation_id is preferred for idempotency if retried
        workflow_id = command.correlation_id or command.command_id
        
        try:
            # Create orchestrator
            orchestrator = TemporalOrchestrator(
                self.client, 
                self.workflow_name, 
                self.task_queue
            )
            
            # Wrap payload cleanly
            # We do NOT mutate command.to_dict()
            payload = WorkflowPayload(
                workflow_id=workflow_id,
                data=command.to_dict()
            )
            
            # Run workflow
            result_id = await orchestrator.run(payload)
            
            return CommandResponse(
                result=result_id,
                correlation_id=workflow_id
            )
        except Exception as e:
            logger.error(f"Failed to start workflow {self.workflow_name}: {e}")
            raise


class TemporalSagaManager:
    """
    Saga Manager that bridges Domain Events to Temporal Workflows.
    
    Instead of executing a local Python Saga class, it signals a Temporal Workflow.
    Uses signal_with_start_workflow to ensure robustness (Choreography pattern),
    automatically starting the workflow if it doesn't exist.
    """

    def __init__(
        self, 
        client: Client, 
        workflow_mapping: Dict[Type[DomainEvent], str],
        task_queue: str,
        id_mapper: Optional[Dict[Type[DomainEvent], str]] = None
    ):
        """
        Args:
            client: Temporal Client
            workflow_mapping: Map DomainEvent types to Workflow Names.
            task_queue: Default Task Queue for starting workflows.
            id_mapper: Optional map of EventType -> Attribute Name for WorkflowID.
        """
        if not HAS_TEMPORAL:
            raise ImportError("temporalio is required")
            
        self.client = client
        self.workflow_mapping = workflow_mapping
        self.task_queue = task_queue
        self.id_mapper = id_mapper or {}

    async def handle_event(self, event: DomainEvent) -> None:
        """
        Handle a domain event by signaling the corresponding Temporal Workflow.
        """
        event_type = type(event)
        workflow_type = self.workflow_mapping.get(event_type)
        
        if not workflow_type:
            return
            
        # Determine Workflow ID
        workflow_id = self._get_workflow_id(event)
        if not workflow_id:
            logger.warning(f"Could not determine Workflow ID for event {event_type.__name__}")
            return

        signal_name = event_type.__name__
        event_data = self._serialize_event(event)
        
        try:
            # Signal With Start
            # This is the "Choreography" enabler: logic doesn't care if it's new or existing.
            # It sends the signal, and if the workflow isn't running, it starts it first.
            await self.client.signal_with_start_workflow(
                workflow=workflow_type,
                workflow_id=workflow_id,
                task_queue=self.task_queue,
                signal=signal_name,
                signal_args=[event_data],
                start_args=[], # Workflow must support starting with no args
            )
            logger.debug(f"Signaled (with start) workflow {workflow_id} with {signal_name}")
            
        except Exception as e:
            logger.error(f"Failed to signal workflow {workflow_id}: {e}")
            raise

    def _get_workflow_id(self, event: DomainEvent) -> Optional[str]:
        """Extract workflow ID from event based on configuration."""
        # 1. Check custom mapper
        attr_name = self.id_mapper.get(type(event))
        if attr_name and hasattr(event, attr_name):
            return str(getattr(event, attr_name))
            
        # 2. Default: correlation_id
        if hasattr(event, "correlation_id") and event.correlation_id:
            return event.correlation_id
            
        # 3. Fallback: aggregate_id
        if hasattr(event, "aggregate_id") and event.aggregate_id:
            return event.aggregate_id
            
        return None

    def _serialize_event(self, event: DomainEvent) -> Dict[str, Any]:
        """Serialize event to dict."""
        if hasattr(event, "to_dict"):
            return event.to_dict()
        if dataclasses.is_dataclass(event):
            return dataclasses.asdict(event)
        return event.__dict__



try:
    from ..orchestration import AbstractOrchestrator
    
    @dataclasses.dataclass
    class WorkflowPayload:
        """Wrapper to pass ID separate from payload."""
        workflow_id: str
        data: Dict[str, Any]

    class TemporalOrchestrator(AbstractOrchestrator[Union[Dict[str, Any], WorkflowPayload], str]):
        """
        Orchestrator implementation using Temporal.
        
        starts a workflow and returns the workflow ID.
        """
        
        def __init__(self, client: Client, workflow_name: str, task_queue: str):
            self.client = client
            self.workflow_name = workflow_name
            self.task_queue = task_queue
            
        async def run(self, input_data: Union[Dict[str, Any], WorkflowPayload]) -> str:
            if not HAS_TEMPORAL:
                 raise ImportError("temporalio is required")
                 
            # Extract ID and Payload
            if isinstance(input_data, WorkflowPayload):
                workflow_id = input_data.workflow_id
                # Unwrap: We send only the inner data to the workflow logic
                args = [input_data.data] 
            else:
                raise ValueError("TemporalOrchestrator requires input_data to be a WorkflowPayload. Use WorkflowPayload(workflow_id=..., data=...)")
            
            handle = await self.client.start_workflow(
                self.workflow_name,
                args=args,
                id=workflow_id,
                task_queue=self.task_queue
            )
            
            logger.info(f"Started orchestration {self.workflow_name} (ID: {workflow_id})")
            return handle.id

except ImportError:
    # If orchestration module or uuid is not available/circular dep
    pass


def import_uuid():
    import uuid
    return uuid

