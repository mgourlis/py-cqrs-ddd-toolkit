"""
Temporal integration for CQRS/DDD toolkit.

Allows using Temporal Workflows as Process Managers/Sagas.
Connects Temporal to the MediatR-style dispatching of this toolkit.
"""

import logging
from typing import Any, Dict, Type
import dataclasses

from ..core import Command, CommandHandler, CommandResponse
from ..ddd import DomainEvent
from ..mediator import Mediator
from ..saga import Saga, SagaChoreographyManager, SagaOrchestratorManager
from ..protocols import SagaRegistry

logger = logging.getLogger("cqrs_ddd.contrib.temporal")

try:
    from temporalio import activity
    from temporalio.client import Client

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

    def __init__(
        self, mediator: Mediator, command_registry: Dict[str, Type[Command]] = None
    ):
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


@dataclasses.dataclass
class WorkflowPayload:
    """Wrapper to pass ID separate from payload."""

    workflow_id: str
    data: Dict[str, Any]


class TemporalOrchestrator:
    """
    Abstract base class for Temporal Orchestrators.
    """

    pass


class TemporalSagaWrapper(Saga):
    """
    Wrapper for using Sagas within Temporal if needed.
    """

    pass


class TemporalSagaChoreographyManager(SagaChoreographyManager):
    """
    Saga Manager for Event-Driven Temporal Workflows (Choreography).

    Instead of processing a local Saga, it signals a Temporal Workflow.
    Uses 'Signal-With-Start' pattern to ensure the workflow is running.
    """

    def __init__(self, client: Client, task_queue: str, saga_registry: SagaRegistry):
        """
        Args:
            client: Temporal Client
            task_queue: Default Task Queue for starting workflows.
            saga_registry: Registry to use for resolving Sagas.
        """
        # We don't need repository or mediator for this manager, as it offloads to Temporal
        super().__init__(repository=None, mediator=None, saga_registry=saga_registry)

        if not HAS_TEMPORAL:
            raise ImportError("temporalio is required")

        self.client = client
        self.task_queue = task_queue

    async def handle_event(self, event: DomainEvent) -> None:
        """
        Handle a domain event by signaling the corresponding Temporal Workflow.
        """
        event_type = type(event)

        # 1. Resolve interested Sagas (Workflows)
        # Use the registry
        saga_classes = self.saga_registry.get_sagas_for_event(event_type)

        if not saga_classes:
            return

        correlation_id = getattr(event, "correlation_id", None)
        if not correlation_id:
            logger.warning(
                f"Event {event_type.__name__} missing correlation_id, skipping Temporal signal."
            )
            return

        event_data = self._serialize_event(event)

        for saga_class in saga_classes:
            # We assume the Saga Class Name maps 1:1 to the Workflow Name
            # Or the Saga class IS the Workflow class (decorated with @workflow.defn)
            # If it's a python class decorated with @workflow.defn, __name__ is correct.
            workflow_name = saga_class.__name__

            # Workflow ID strategy: SagaName-CorrelationID
            workflow_id = f"{workflow_name}-{correlation_id}"

            # Signal Name: Event Name
            signal_name = event_type.__name__

            try:
                # Signal With Start
                await self.client.signal_with_start_workflow(
                    workflow=workflow_name,
                    workflow_id=workflow_id,
                    task_queue=self.task_queue,
                    signal=signal_name,
                    signal_args=[event_data],
                    start_args=[],  # Workflow must support starting with no args if triggered by event
                )
                logger.debug(
                    f"Signaled (with start) workflow {workflow_id} with {signal_name}"
                )

            except Exception as e:
                logger.error(f"Failed to signal workflow {workflow_id}: {e}")
                # We do NOT re-raise to avoid blocking other handlers?
                # Actually, blocking is safer for data integrity.
                raise

    def _serialize_event(self, event: DomainEvent) -> Dict[str, Any]:
        """Serialize event to dict."""
        if hasattr(event, "to_dict"):
            return event.to_dict()
        if dataclasses.is_dataclass(event):
            return dataclasses.asdict(event)
        return event.__dict__


class TemporalSagaOrchestratorManager(SagaOrchestratorManager):
    """
    Saga Manager for Explicit Temporal Orchestrations.
    """

    def __init__(self, client: Client, task_queue: str, saga_registry: SagaRegistry):
        # No repo/mediator needed locally
        super().__init__(repository=None, mediator=None, saga_registry=saga_registry)

        if not HAS_TEMPORAL:
            raise ImportError("temporalio is required")

        self.client = client
        self.task_queue = task_queue

    async def run(
        self, saga_class: Type[Saga], input_data: Any, correlation_id: str
    ) -> str:
        """
        Explicitly start a Temporal Workflow (Saga).
        """
        workflow_name = saga_class.__name__
        workflow_id = f"{workflow_name}-{correlation_id}"

        # Serialize input
        if dataclasses.is_dataclass(input_data):
            data = dataclasses.asdict(input_data)
        elif isinstance(input_data, dict):
            data = input_data
        else:
            data = input_data

        # Optional: Wrap if the workflow expects a specific signature
        # We assume the workflow takes 'data' as first arg
        args = [data]

        try:
            handle = await self.client.start_workflow(
                workflow=workflow_name,
                args=args,
                id=workflow_id,
                task_queue=self.task_queue,
            )
            logger.info(
                f"Started Temporal Orchestration {workflow_name} (ID: {workflow_id})"
            )
            return handle.id

        except Exception as e:
            logger.error(f"Failed to start workflow {workflow_name}: {e}")
            raise


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
        workflow_id = command.correlation_id or command.command_id

        # Use the Orchestrator Manager to ensure consistent logic
        # But wait, this handler might want more granular control?
        # Ideally, we just use the manager logic.

        # We need to construct a "Dummy" Saga class or just use the client directly?
        # The OrchestratorManager expects a Class.
        # Let's use the client directly to avoid needing a fake class,
        # matching the old implementation but cleaner.

        try:
            # Wrap payload cleanly using WorkflowPayload if desired,
            # Or just raw dict. Old implementation used WorkflowPayload.
            # Let's stick to raw dict command data for simplicity,
            # OR wrap it if the workflow expects (workflow_id, data).

            # Based on user "remove legacy", we should simplify.
            # Passing raw data is standard.

            args = [command.to_dict()]

            handle = await self.client.start_workflow(
                self.workflow_name,
                args=args,
                id=workflow_id,
                task_queue=self.task_queue,
            )

            return CommandResponse(result=handle.id, correlation_id=workflow_id)
        except Exception as e:
            logger.error(f"Failed to start workflow {self.workflow_name}: {e}")
            raise
