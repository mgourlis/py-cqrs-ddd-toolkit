"""Saga pattern implementation for long-running processes."""

from abc import ABC
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Generic,
    TYPE_CHECKING,
    Union,
    get_origin,
    get_args,
)
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
import logging
import uuid
import dataclasses
import importlib

from .core import Command
from .ddd import DomainEvent
from .domain_event import DomainEventBase
# from .saga_registry import saga_registry # Deprecated global registry dependency

if TYPE_CHECKING:
    from .mediator import Mediator
    from .protocols import LockStrategy

from .protocols import SagaRepository, SagaRegistry


# Try to import Tenacity for retries
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        before_sleep_log,
    )

    HAS_TENACITY = True
except ImportError:
    HAS_TENACITY = False

logger = logging.getLogger("cqrs_ddd")

TState = TypeVar("TState")


@dataclass
class SagaContext:
    """State data for a saga instance."""

    saga_id: str
    saga_type: str
    correlation_id: str
    current_step: str
    state: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    # Stack ofserialized commands for compensation
    compensations: List[Dict[str, Any]] = field(default_factory=list)
    # Failed compensations for manual intervention
    failed_compensations: List[Dict[str, Any]] = field(default_factory=list)
    # Queue of serialized commands waiting to be dispatched (for transactional safety)
    pending_commands: List[Dict[str, Any]] = field(default_factory=list)

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error: Optional[str] = None
    is_completed: bool = False
    is_failed: bool = False
    # Stalled indicates retries exhausted but not explicitly failed via logic
    is_stalled: bool = False
    processed_message_ids: List[str] = field(default_factory=list)

    # Human-in-the-Loop / Suspension Support
    is_suspended: bool = False
    suspend_reason: Optional[str] = None
    suspend_timeout_at: Optional[datetime] = None
    wait_for_event: Optional[str] = None


class Saga(ABC, Generic[TState]):
    """
    Base class for Sagas (Process Managers).

    A Saga coordinates long-running flows by listening to events and
    issuing commands. It maintains state across multiple transactions.

    Supports Typed State via Generics:
        class OrderSaga(Saga[OrderState]): ...
    """

    _handlers: Dict[Type, str] = {}

    def __init_subclass__(cls, **kwargs):
        """Build handler map from decorated methods."""
        super().__init_subclass__(**kwargs)

        # Inherit handlers from base classes
        cls._handlers = {}
        for base in reversed(cls.__mro__):
            if hasattr(base, "_handlers"):
                cls._handlers.update(base._handlers)

        # Discover new handlers in current class
        for name, attr in cls.__dict__.items():
            if hasattr(attr, "_saga_event_type"):
                event_type = attr._saga_event_type
                cls._handlers[event_type] = name

        # Register type (always)
        # Note: We rely on the user to register the Saga with their specific Registry instance.
        # Global auto-registration is deprecated/removed to support isolation.

        # Original auto-discovery logic removed to enforce Dependency Injection.
        # pass

    def __init__(self, context: SagaContext, mediator: "Mediator"):
        self.context = context
        self.mediator = mediator
        # Local cache of commands to dispatch, will be moved to context.pending_commands before save
        self._local_pending_commands: List[Command] = []

        # Initialize Typed State
        self._state: Optional[TState] = None
        self._load_state()

    def _load_state(self):
        """Load typed state from context dictionary."""
        # Detect TState type from __orig_bases__ if possible (Python runtime introspection)
        # OR explicit state_type attribute
        state_type = getattr(self, "state_type", None) or self._get_state_type()

        if state_type and dataclasses.is_dataclass(state_type):
            # Zero-dep Dataclass support
            if self.context.state:
                try:
                    self._state = state_type(**self.context.state)
                except Exception as e:
                    # Strict Fallback: Do NOT hide data corruption.
                    logger.error(
                        f"Failed to parse existing state into dataclass {state_type}: {e}"
                    )
                    raise ValueError(
                        f"Saga State Corruption: Could not bind context to {state_type.__name__}"
                    ) from e
            else:
                try:
                    self._state = state_type()
                except Exception:
                    self._state = None
        else:
            # Fallback for dict
            self._state = self.context.state

    def _get_state_type(self) -> Optional[Type]:
        """Introspect generic type TState."""
        # Use standard typing introspection (Python 3.8+)
        # We need to traverse MRO to find where Saga[T] was inherited
        for cls in type(self).__mro__:
            if hasattr(cls, "__orig_bases__"):
                for base in cls.__orig_bases__:
                    origin = get_origin(base)
                    if origin is Saga:
                        args = get_args(base)
                        if args:
                            return args[0]
        return None

    @property
    def state(self) -> Union[TState, Dict[str, Any]]:
        """Access the typed state."""
        return self._state if self._state is not None else self.context.state

    @state.setter
    def state(self, value: TState):
        self._state = value

    def _sync_state(self):
        """Sync typed state back to context.state dict before save."""
        if dataclasses.is_dataclass(self._state):
            self.context.state = asdict(self._state)
        elif isinstance(self._state, dict):
            self.context.state = self._state

    @property
    def id(self) -> str:
        return self.context.saga_id

    @property
    def is_active(self) -> bool:
        return not (self.context.is_completed or self.context.is_failed)

    async def handle_event(self, event: DomainEvent) -> None:
        """Handle an incoming domain event."""
        event_type = type(event)
        handler_name = self._handlers.get(event_type)

        if handler_name and hasattr(self, handler_name):
            handler = getattr(self, handler_name)

            # Update context history
            self.context.history.append(
                {
                    "action": "handle_event",
                    "event_type": event_type.__name__,
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            self.context.updated_at = datetime.now(timezone.utc)

            try:
                try:
                    await handler(event)
                except Exception as e:
                    self.fail(str(e))
                    logger.error(
                        f"Saga {self.id} failed processing {event_type.__name__}: {e}"
                    )
                    # Trigger compensation
                    try:
                        await self.compensate()
                    except Exception as comp_err:
                        logger.critical(
                            f"Compensation failed for Saga {self.id}: {comp_err}"
                        )
                    raise e
            finally:
                # IMPORTANT: We do NOT dispatch here anymore.
                # Queued commands are kept in self._local_pending_commands
                # and will be moved to context.pending_commands by manager before flush
                pass

    def dispatch_command(self, command: Command) -> None:
        """Queue a command to be dispatched."""
        self._local_pending_commands.append(command)

    def add_compensation(self, command: Command) -> None:
        """Register a compensation command (Declarative Compensation)."""
        serialized = self._serialize_command(command)
        self.context.compensations.append(serialized)

    async def execute_compensations(self):
        """Execute registered compensations in LIFO order."""
        while self.context.compensations:
            cmd_data = self.context.compensations.pop()
            try:
                await self._dispatch_serialized_command(cmd_data)
            except Exception as e:
                logger.critical(f"Failed to execute compensation {cmd_data}: {e}")
                # Track failed compensation for manual intervention
                self.context.failed_compensations.append(
                    {
                        "command": cmd_data,
                        "error": str(e),
                        "failed_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                # Best Effort: Continue to next compensation

    def complete(self) -> None:
        """Mark saga as successfully completed."""
        self.context.is_completed = True
        self.context.is_suspended = False  # Clear suspension
        self.context.completed_at = datetime.now(timezone.utc)
        self.context.updated_at = datetime.now(timezone.utc)

    def fail(self, error: str) -> None:
        """Mark saga as failed."""
        self.context.is_failed = True
        self.context.is_suspended = False  # Clear suspension
        self.context.failed_at = datetime.now(timezone.utc)
        self.context.error = error
        self.context.updated_at = datetime.now(timezone.utc)

    async def compensate(self) -> None:
        """
        Execute compensating actions.
        Default implementation uses the Declarative Compensation Stack.
        Override if you need custom logic.
        """
        await self.execute_compensations()

    def suspend(self, reason: str, timeout: Optional[timedelta] = None) -> None:
        """
        Suspend the saga execution.

        Args:
            reason: Reason for suspension (e.g., "Waiting for User Approval")
            timeout: Optional duration after which the saga should timeout.
        """
        self.context.is_suspended = True
        self.context.suspend_reason = reason
        if timeout:
            self.context.suspend_timeout_at = datetime.now(timezone.utc) + timeout
        else:
            self.context.suspend_timeout_at = None
        self.context.updated_at = datetime.now(timezone.utc)

    def resume(self) -> None:
        """Resume saga execution."""
        self.context.is_suspended = False
        self.context.suspend_reason = None
        self.context.suspend_timeout_at = None
        self.context.updated_at = datetime.now(timezone.utc)

    async def on_timeout(self) -> None:
        """
        Handle timeout of suspended saga.
        Default implementation fails the saga. Override for custom logic.
        """
        self.fail(f"Saga suspended for '{self.context.suspend_reason}' timed out.")

    async def start(self, initial_data: Any) -> None:
        """
        Start the saga with initial data (Orchestration entry point).
        """
        self.context.updated_at = datetime.now(timezone.utc)

        # If user passes a Dataclass matching TState
        if dataclasses.is_dataclass(initial_data):
            self._state = initial_data

        # Note: If initial_data is dict, we rely on _load_state or manual assignment
        # The user is responsible for mapping dict to state in their start method if needed.

    def _serialize_command(self, command: Command) -> Dict[str, Any]:
        """Serialize command to dict with type info."""
        data = (
            asdict(command) if dataclasses.is_dataclass(command) else command.__dict__
        )
        return {
            "type_name": type(command).__name__,
            "module_name": type(command).__module__,
            "data": data,
        }

    async def _dispatch_serialized_command(self, cmd_data: Dict[str, Any]) -> None:
        """Reconstruct and dispatch a serialized command."""
        try:
            module_name = cmd_data.get("module_name")
            type_name = cmd_data.get("type_name")
            data = cmd_data.get("data", {})

            if not module_name or not type_name:
                raise ValueError("Invalid serialized command: missing type info")

            module = importlib.import_module(module_name)
            command_class = getattr(module, type_name)

            # Re-instantiate command
            # Assumes standard __init__ accepting kwargs matching data fields
            # or dataclass/pydantic constructor structure
            command = command_class(**data)

            # Restore correlation_id if needed? usually it's in data

            await self.mediator.send(command)
        except Exception as e:
            logger.error(f"Failed to deserialize/dispatch command {cmd_data}: {e}")
            raise e


def saga_step(event_type: Type):
    """
    Decorator to mark a method as a saga step handler.
    """

    def decorator(func):
        func._saga_event_type = event_type
        return func

    return decorator


class InMemorySagaRepository(SagaRepository):
    """In-memory saga repository for testing."""

    def __init__(self):
        self._sagas: Dict[str, SagaContext] = {}
        # (saga_type, correlation_id) -> saga_id
        self._correlation_type_map: Dict[tuple, str] = {}

    async def save(self, context: SagaContext) -> None:
        self._sagas[context.saga_id] = context
        key = (context.saga_type, context.correlation_id)
        self._correlation_type_map[key] = context.saga_id

    async def load(self, saga_id: str) -> Optional[SagaContext]:
        return self._sagas.get(saga_id)

    async def find_by_correlation_id(
        self, correlation_id: str, saga_type: str
    ) -> Optional[SagaContext]:
        key = (saga_type, correlation_id)
        saga_id = self._correlation_type_map.get(key)
        if saga_id:
            return self._sagas.get(saga_id)
        return None

    async def find_stalled_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are stalled."""
        stalled = []
        for ctx in self._sagas.values():
            if ctx.is_stalled:
                stalled.append(ctx)
                if len(stalled) >= limit:
                    break
        return stalled

    async def find_suspended_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are suspended."""
        suspended = []
        for ctx in self._sagas.values():
            if ctx.is_suspended:
                suspended.append(ctx)
                if len(suspended) >= limit:
                    break
        return suspended

    async def find_expired_suspended_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are suspended and have timed out."""
        expired = []
        now = datetime.now(timezone.utc)
        for ctx in self._sagas.values():
            if (
                ctx.is_suspended
                and ctx.suspend_timeout_at
                and ctx.suspend_timeout_at <= now
            ):
                expired.append(ctx)
                if len(expired) >= limit:
                    break
        return expired


class SagaManager(ABC):
    """
    Abstract Base Class for Saga Managers.
    """

    def __init__(
        self,
        repository: SagaRepository,
        mediator: "Mediator",
        saga_registry: SagaRegistry,
        lock_strategy: Optional["LockStrategy"] = None,
    ):
        self.repository = repository
        self.mediator = mediator
        self.saga_registry = saga_registry
        self.lock_strategy = lock_strategy

    def _get_lock_key(self, saga_type: str, correlation_id: str) -> str:
        """Generate standardized lock key."""
        return f"saga:{saga_type}:{correlation_id}"

    async def _dispatch_with_retry(self, command: Any) -> None:
        """Helper to dispatch with optional retry logic."""
        if HAS_TENACITY:
            # Configure retry: Exponential backoff, stop after 3 attempts
            @retry(
                reraise=True,
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )
            async def _send_safe():
                await self.mediator.send(command)

            await _send_safe()
        else:
            await self.mediator.send(command)

    async def _dispatch_and_record_command(
        self, command: Command, context: SagaContext
    ) -> None:
        """
        Dispatch a command with retry logic and record it in saga history.
        Also propagates correlation_id if missing.
        """
        # Propagate correlation ID
        if hasattr(command, "correlation_id") and not command.correlation_id:
            command.correlation_id = context.correlation_id

        await self._dispatch_with_retry(command)

        context.history.append(
            {
                "action": "dispatch_command",
                "command_type": type(command).__name__,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    async def recover_pending_sagas(self, limit: int = 10):
        """
        Recover sagas that have pending commands but halted (crash recovery).
        """
        # 1. Find stalled sagas
        # We assume repository has find_stalled_sagas (added to protocol)
        if not hasattr(self.repository, "find_stalled_sagas"):
            logger.warning(
                "SagaRepository does not support find_stalled_sagas, skipping recovery."
            )
            return

        stalled_contexts = await self.repository.find_stalled_sagas(limit)

        for context in stalled_contexts:
            logger.info(
                f"Recovering stalled saga {context.saga_id} with {len(context.pending_commands)} pending commands."
            )

            try:
                # We reuse the logic: dispatch -> ack -> save
                processed_count = 0

                # Copy pending commands to iterate safely (we might modify list on ack)
                cmds_to_dispatch = list(context.pending_commands)

                for cmd_data in cmds_to_dispatch:
                    # Deserialize and Dispatch
                    try:
                        module_name = cmd_data.get("module_name")
                        type_name = cmd_data.get("type_name")
                        data = cmd_data.get("data", {})

                        if module_name and type_name:
                            module = importlib.import_module(module_name)
                            command_class = getattr(module, type_name)
                            # Re-instantiate
                            command = command_class(**data)
                            # Retry Dispatch
                            await self._dispatch_with_retry(command)
                            processed_count += 1
                        else:
                            logger.error(
                                f"Invalid serialized command in Saga {context.saga_id}: {cmd_data}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to recover command in Saga {context.saga_id}: {e}"
                        )
                        raise e  # Re-raise to break loop and keep stalled

                # If all successful:
                if processed_count == len(cmds_to_dispatch):
                    context.pending_commands.clear()
                    context.is_stalled = False
                    context.updated_at = datetime.now(timezone.utc)
                    await self.repository.save(context)
                    logger.info(f"Successfully recovered Saga {context.saga_id}")

            except Exception as e:
                logger.error(f"Recovery failed for Saga {context.saga_id}: {e}")
                # Remains stalled

    async def process_timeouts(self, limit: int = 10):
        """
        Process sagas that have been suspended and timed out.
        """
        if not hasattr(self.repository, "find_expired_suspended_sagas"):
            return

        expired_contexts = await self.repository.find_expired_suspended_sagas(limit)

        for context in expired_contexts:
            logger.info(
                f"Processing timeout for Saga {context.saga_id} (Reason: {context.suspend_reason})"
            )

            lock_token = None
            lock_token = None
            if self.lock_strategy:
                # Use standardized lock key
                lock_key = self._get_lock_key(context.saga_type, context.correlation_id)
                try:
                    lock_token = await self.lock_strategy.acquire("saga", lock_key)
                except Exception as e:
                    logger.warning(
                        f"Could not acquire lock for timeout processing {context.saga_id}: {e}"
                    )
                    continue

            try:
                # Reload to be safe under lock
                context = await self.repository.load(context.saga_id)
                if not context or not context.is_suspended:
                    continue

                # Double check timeout
                now = datetime.now(timezone.utc)
                if not context.suspend_timeout_at or context.suspend_timeout_at > now:
                    continue

                # Find Saga Class and restore state
                saga_class = self._find_saga_class_by_name(context.saga_type)
                if not saga_class:
                    logger.error(
                        f"Could not find Saga class {context.saga_type} for timeout processing"
                    )
                    continue

                saga = saga_class(context, self.mediator)

                try:
                    await saga.on_timeout()
                except Exception as e:
                    logger.error(f"Error in on_timeout for Saga {context.saga_id}: {e}")
                    # Force fail if on_timeout crashes?
                    if not context.is_failed:
                        saga.fail(f"Timeout handler failed: {e}")

                saga._sync_state()

                # Check if still suspended? If on_timeout didn't change it, we should probably un-suspend or fail?
                # Default on_timeout fails, so is_failed=True, is_suspended irrelevant.
                # If they resumed, is_suspended=False.
                # If they just logged and did nothing? Infinite loop of timeouts?
                # We should force clear timeout if it wasn't cleared.
                if (
                    context.is_suspended
                    and context.suspend_timeout_at
                    and context.suspend_timeout_at <= now
                ):
                    # Avoid infinite loop
                    logger.warning(
                        f"Saga {context.saga_id} on_timeout did not resolve suspension. Forcing failure."
                    )
                    saga.fail("Timeout handler did not resolve suspension")

                await self.repository.save(context)

                # If they queued compensations or commands

                # Move newly generated commands from Saga instance to context
                for cmd in saga._local_pending_commands:
                    context.pending_commands.append(saga._serialize_command(cmd))

                # Dispatch if any commands exist (from previous execution or new ones)
                if context.pending_commands:
                    await self._dispatch_pending_commands(context)

            except Exception as e:
                logger.error(
                    f"Failed to process timeout for saga {context.saga_id}: {e}"
                )
            finally:
                if self.lock_strategy and lock_token:
                    lock_key = self._get_lock_key(
                        context.saga_type, context.correlation_id
                    )
                    await self.lock_strategy.release("saga", lock_key, lock_token)

    def _find_saga_class_by_name(self, name: str) -> Optional[Type["Saga"]]:
        """Helper to find saga class by name using efficient registry lookup."""
        return self.saga_registry.get_saga_type(name)

    async def _dispatch_pending_commands(self, context: SagaContext):
        cmds_to_dispatch = list(context.pending_commands)
        for cmd_data in cmds_to_dispatch:
            try:
                module_name = cmd_data.get("module_name")
                type_name = cmd_data.get("type_name")
                data = cmd_data.get("data", {})
                if module_name and type_name:
                    module = importlib.import_module(module_name)
                    command_class = getattr(module, type_name)
                    command = command_class(**data)

                    await self._dispatch_and_record_command(command, context)

            except Exception as e:
                logger.error(
                    f"Failed to dispatch pending command for saga {context.saga_id}: {e}"
                )
                # We continue trying other commands? Or fail?
                # Current logic was 'pass', we should probably log at least.

        context.pending_commands.clear()
        await self.repository.save(context)

    async def _process_saga(
        self,
        saga_class: Type["Saga"],
        correlation_id: str,
        event: Optional[DomainEvent] = None,
        input_data: Any = None,
    ) -> Any:
        try:
            lock_token = None
            lock_token = None
            if self.lock_strategy:
                # Use name for consistent key generation with context.saga_type
                lock_key = self._get_lock_key(saga_class.__name__, correlation_id)
                lock_token = await self.lock_strategy.acquire("saga", lock_key)

            try:
                saga_type_name = saga_class.__name__
                context = await self.repository.find_by_correlation_id(
                    correlation_id, saga_type=saga_type_name
                )

                if not context:
                    context = SagaContext(
                        saga_id=str(uuid.uuid4()),
                        saga_type=saga_type_name,
                        correlation_id=correlation_id,
                        current_step="start",
                    )
                    await self.repository.save(context)

                if context.is_completed or context.is_failed:
                    return None

                # If Stalled, we might be retrying manually? For now, we allow re-processing.
                if context.is_stalled:
                    logger.info(f"Resuming STALLED saga {context.saga_id}")
                    context.is_stalled = False

                # Idempotency check for events
                event_id = None
                if event:
                    event_id = getattr(event, "event_id", getattr(event, "id", None))
                    if event_id and event_id in context.processed_message_ids:
                        logger.info(
                            f"Saga {context.saga_id} already processed event {event_id}"
                        )
                        return None

                # Instantiate Saga
                saga = saga_class(context, self.mediator)

                # Execute Logic (mutates state, queues commands in memory)
                try:
                    if event:
                        await saga.handle_event(event)
                    elif input_data is not None:
                        await saga.start(input_data)
                except Exception:
                    # If generic error in handler, we persist the failure state
                    saga._sync_state()  # Ensure typed state is saved back

                    # Capture queued commands into context for persistence/recovery
                    for cmd in saga._local_pending_commands:
                        context.pending_commands.append(saga._serialize_command(cmd))

                    await self.repository.save(context)
                    raise

                # Capture queued commands into context for persistence
                # We serialize them now
                for cmd in saga._local_pending_commands:
                    context.pending_commands.append(saga._serialize_command(cmd))

                if event_id:
                    context.processed_message_ids.append(event_id)

                # Sync typed state
                saga._sync_state()

                # 1. SAVE State + Pending Commands (Transactional Safety point 1)
                await self.repository.save(context)

                # 2. DISPATCH (At-least-once) with Retry
                # We reuse the command instances in saga._local_pending_commands for now
                try:
                    for cmd in saga._local_pending_commands:
                        await self._dispatch_and_record_command(cmd, context)
                except Exception as dispatch_err:
                    logger.error(
                        f"Saga {context.saga_id} stalled during dispatch: {dispatch_err}"
                    )
                    # Mark as STALLED, do NOT fail/compensate yet.
                    # Commands are still in 'pending_commands' in DB for retry (via recover_pending_sagas)
                    context.is_stalled = True
                    context.error = str(dispatch_err)
                    context.updated_at = datetime.now(timezone.utc)
                    await self.repository.save(context)
                    raise dispatch_err

                # 3. ACK Commands (remove from pending) & SAVE
                context.pending_commands.clear()
                context.updated_at = datetime.now(timezone.utc)
                await self.repository.save(context)

                return context.saga_id

            finally:
                if self.lock_strategy and lock_token:
                    lock_key = self._get_lock_key(saga_class.__name__, correlation_id)
                    await self.lock_strategy.release("saga", lock_key, lock_token)

        except Exception as e:
            logger.error(f"Error processing Saga {saga_class.__name__}: {e}")
            raise


class SagaChoreographyManager(SagaManager):
    """
    Manages event-driven Sagas (Choreography).
    """

    async def handle_event(self, event: DomainEvent) -> None:
        event_type = type(event)

        # Registry is now mandatory and enforced in __init__
        saga_classes = self.saga_registry.get_sagas_for_event(event_type)

        if not saga_classes:
            return

        correlation_id = getattr(event, "correlation_id", None)
        if not correlation_id:
            if hasattr(event, "metadata") and "correlation_id" in event.metadata:
                correlation_id = event.metadata["correlation_id"]
            else:
                logger.warning(
                    f"Event {event_type.__name__} has no correlation_id, cannot match saga"
                )
                return

        for saga_class in saga_classes:
            await self._process_saga(saga_class, correlation_id, event=event)

    async def handle(self, event: DomainEvent) -> None:
        """Alias for handle_event to satisfy EventDispatcher requirements."""
        await self.handle_event(event)


class SagaOrchestratorManager(SagaManager):
    """
    Manages explicit Saga Orchestrations.
    """

    async def run(
        self, saga_class: Type["Saga"], input_data: Any, correlation_id: str
    ) -> str:
        # If input data is a DomainEvent (or Command), treat it as an event handling step
        if isinstance(input_data, (DomainEvent, DomainEventBase)):
            await self._process_saga(saga_class, correlation_id, event=input_data)
            return correlation_id

        # Otherwise, treat it as raw input data for the Saga.start() method
        return await self._process_saga(
            saga_class, correlation_id, event=None, input_data=input_data
        )
