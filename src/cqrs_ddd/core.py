"""Core CQRS components - Commands, Queries, Handlers, Responses."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import logging
from typing import TypeVar, Generic, Any, List, Optional, TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass

T = TypeVar("T")
TResult = TypeVar("TResult")
T_Event = TypeVar("T_Event")

# === Global Dispatcher Registry ===
# Removed: Global persistence dispatcher resolver is deprecated. Use Dependency Injection.


def import_uuid():
    """Lazy import for uuid."""
    import uuid

    return uuid


class AbstractCommand:
    """
    Marker interface for all Commands.

    Commands represent an intent to change the state of the system.
    They should be named in the imperative, e.g., 'CreateUser', 'UpdateProfile'.
    """

    pass


class AbstractQuery:
    """
    Marker interface for all Queries.

    Queries request information from the system without modifying state.
    They should be named to describe the data needed, e.g., 'GetUser', 'ListOrders'.
    """

    pass


class AbstractDomainEvent:
    """
    Marker interface for all Domain Events.

    Events represent something that has happened in the past.
    They should be named in the past tense, e.g., 'UserCreated', 'OrderShipped'.
    """

    pass


class AbstractOutboxMessage:
    """Marker class for all outbox messages."""

    id: Any
    occurred_at: Any
    type: str
    topic: str
    payload: dict
    correlation_id: Optional[str]
    status: str
    retries: int
    error: Optional[str]
    processed_at: Optional[Any]


@dataclass(kw_only=True)
class Command(AbstractCommand):
    """
    Base dataclass for Commands.

    Includes auto-generated unique ID and correlation tracking.
    Users should inherit from this and use @dataclass decorator.
    """

    _command_id: str = field(
        default_factory=lambda: str(import_uuid().uuid4()), repr=False
    )
    _correlation_id: Optional[str] = field(default=None, repr=False)

    @property
    def command_id(self) -> str:
        """Get command ID."""
        return self._command_id

    @property
    def correlation_id(self) -> Optional[str]:
        """Get correlation ID."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: Optional[str]):
        """Set correlation ID."""
        self._correlation_id = value

    def to_dict(self) -> dict:
        """Convert command to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


@dataclass(kw_only=True)
class Query(AbstractQuery):
    """
    Base dataclass for Queries.

    Includes auto-generated unique ID and correlation tracking.
    Users should inherit from this and use @dataclass decorator.
    """

    _query_id: str = field(
        default_factory=lambda: str(import_uuid().uuid4()), repr=False
    )
    _correlation_id: Optional[str] = field(default=None, repr=False)

    @property
    def query_id(self) -> str:
        """Get query ID."""
        return self._query_id

    @property
    def correlation_id(self) -> Optional[str]:
        """Get correlation ID."""
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: Optional[str]):
        """Set correlation ID."""
        self._correlation_id = value

    def to_dict(self) -> dict:
        """Convert query to dictionary."""
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


class CommandHandler(ABC, Generic[TResult]):
    """
    Base class for handling Commands.

    Each handler handles exactly one command type.

    Features:
    - Auto-registration: Subclasses are automatically registered as handlers
      based on the type annotation of the `handle` method first argument.
    """

    def __init__(self, **kwargs):
        """
        Initialize handler.
        """
        pass

    @abstractmethod
    async def handle(self, command: Command) -> "CommandResponse[TResult]":
        """Handle the command and return a response."""
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """Auto-register handler based on handle method signature."""
        super().__init_subclass__(**kwargs)

        if ABC in cls.__bases__:
            return

        import inspect
        from .handler_registry import register_command_handler

        if hasattr(cls, "handle"):
            try:
                sig = inspect.signature(cls.handle)
                params = list(sig.parameters.values())
                # Expect handle(self, command: Type)
                if len(params) >= 2:
                    cmd_param = params[1]
                    if (
                        cmd_param.annotation is not inspect.Parameter.empty
                        and isinstance(cmd_param.annotation, type)
                    ):
                        # Validate it is a Command (using marker interface)
                        if issubclass(cmd_param.annotation, AbstractCommand):
                            register_command_handler(cmd_param.annotation, cls)
                            cls._handles_command = cmd_param.annotation
                        else:
                            logger.warning(
                                f"Could not register {cls.__name__}: {cmd_param.annotation.__name__} does not inherit from AbstractCommand"
                            )
                    else:
                        logger.warning(
                            f"Could not register {cls.__name__}: First argument of handle() must have a type annotation"
                        )
                else:
                    logger.warning(
                        f"Could not register {cls.__name__}: handle() method must accept at least one argument (besides self)"
                    )
            except Exception as e:
                logger.warning(f"Error during auto-registration of {cls.__name__}: {e}")


class QueryHandler(ABC, Generic[TResult]):
    """
    Base class for handling Queries.

    Each handler handles exactly one query type.

    Features:
    - Auto-registration: Subclasses are automatically registered as handlers
      based on the type annotation of the `handle` method first argument.
    """

    def __init__(self, **kwargs):
        """Initialize handler."""
        pass

    @abstractmethod
    async def handle(self, query: Query) -> "QueryResponse[TResult]":
        """Handle the query and return a response."""
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """Auto-register handler based on handle method signature."""
        super().__init_subclass__(**kwargs)

        if ABC in cls.__bases__:
            return

        import inspect
        from .handler_registry import register_query_handler

        if hasattr(cls, "handle"):
            try:
                sig = inspect.signature(cls.handle)
                params = list(sig.parameters.values())
                if len(params) >= 2:
                    query_param = params[1]
                    if (
                        query_param.annotation is not inspect.Parameter.empty
                        and isinstance(query_param.annotation, type)
                    ):
                        # Validate it is a Query
                        if issubclass(query_param.annotation, AbstractQuery):
                            register_query_handler(query_param.annotation, cls)
                            cls._handles_query = query_param.annotation
                        else:
                            logger.warning(
                                f"Could not register {cls.__name__}: {query_param.annotation.__name__} does not inherit from AbstractQuery"
                            )
                    else:
                        logger.warning(
                            f"Could not register {cls.__name__}: First argument of handle() must have a type annotation"
                        )
                else:
                    logger.warning(
                        f"Could not register {cls.__name__}: handle() method must accept at least one argument (besides self)"
                    )
            except Exception as e:
                logger.warning(f"Error during auto-registration of {cls.__name__}: {e}")


class EventHandler(ABC, Generic[T_Event]):
    """
    Base class for handling Domain Events.

    Can handle one or more event types (via Union type hint).

    Attributes:
        is_priority (bool):
            - True: Runs synchronously in the same transaction as the command. (Use caution!)
            - False: Runs in the background/asynchronously after transaction commit. (Default)
    """

    is_priority: bool = False

    @abstractmethod
    async def handle(self, event: T_Event) -> None:
        """Handle the domain event."""
        raise NotImplementedError

    def __init_subclass__(cls, **kwargs):
        """Auto-register handler based on handle method signature."""
        super().__init_subclass__(**kwargs)

        if ABC in cls.__bases__:
            return

        import inspect
        from typing import get_origin, get_args, Union

        try:
            from types import UnionType
        except ImportError:
            UnionType = Union

        from .handler_registry import register_event_handler

        if hasattr(cls, "handle"):
            try:
                sig = inspect.signature(cls.handle)
                params = list(sig.parameters.values())
                if len(params) >= 2:
                    event_param = params[1]
                    annotation = event_param.annotation

                    if annotation is not inspect.Parameter.empty:
                        origin = get_origin(annotation)
                        if origin in (Union, UnionType):
                            # Handle Union[Event1, Event2]
                            event_types = get_args(annotation)
                            for et in event_types:
                                if et is not type(None):
                                    register_event_handler(
                                        et, cls, getattr(cls, "is_priority", False)
                                    )
                            # For internal tracking, store the first one or a tuple
                            cls._handles_event = event_types[0]
                        elif isinstance(annotation, type):
                            # Handle single Event
                            register_event_handler(
                                annotation, cls, getattr(cls, "is_priority", False)
                            )
                            cls._handles_event = annotation
                        else:
                            logger.warning(
                                f"Could not register {cls.__name__}: Invalid type annotation {annotation}"
                            )
                    else:
                        logger.warning(
                            f"Could not register {cls.__name__}: First argument of handle() must have a type annotation"
                        )
                else:
                    logger.warning(
                        f"Could not register {cls.__name__}: handle() method must accept at least one argument (besides self)"
                    )
            except Exception as e:
                logger.warning(f"Error during auto-registration of {cls.__name__}: {e}")


@dataclass
class CommandResponse(Generic[TResult]):
    """
    Response from a command handler.

    Contains the result and any domain events to dispatch.
    """

    result: TResult
    events: List[Any] = field(default_factory=list)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None

    @property
    def response(self) -> TResult:
        """Alias for result (backward compatibility)."""
        return self.result

    @property
    def domain_events(self) -> List[Any]:
        """Alias for events (backward compatibility)."""
        return self.events


@dataclass
class QueryResponse(Generic[TResult]):
    """Response from a query handler."""

    result: TResult

    @property
    def response(self) -> TResult:
        """Alias for result (backward compatibility)."""
        return self.result
