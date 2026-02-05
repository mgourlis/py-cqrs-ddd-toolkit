"""Pydantic integration for CQRS/DDD toolkit."""

from typing import Any, Dict, Type, TypeVar, Generic, Optional
from dataclasses import dataclass
from ..core import (
    AbstractCommand,
    AbstractQuery,
    AbstractDomainEvent,
    AbstractOutboxMessage,
)
from ..saga import Saga

try:
    from pydantic import BaseModel, Field, PrivateAttr

    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object
    Field = None


# =============================================================================
# Pydantic-based Commands and Queries
# =============================================================================

if HAS_PYDANTIC:

    class PydanticCommand(BaseModel, AbstractCommand):
        """
        Base class for Pydantic-validated commands.

        Combines Pydantic validation with CQRS Command semantics.

        Usage:
            class CreateUser(PydanticCommand):
                name: str = Field(..., min_length=1, max_length=100)
                email: str = Field(..., pattern=r'^[\\w.-]+@[\\w.-]+\\.\\w+$')

                @field_validator('name')
                @classmethod
                def name_not_reserved(cls, v):
                    if v.lower() in ['admin', 'root']:
                        raise ValueError("Reserved name")
                    return v
        """

        # Identification (Command protocol compatibility)
        _command_id: Optional[str] = PrivateAttr(
            default_factory=lambda: str(import_uuid().uuid4())
        )
        _correlation_id: Optional[str] = PrivateAttr(default=None)

        @property
        def command_id(self) -> str:
            return self._command_id

        @property
        def correlation_id(self) -> Optional[str]:
            return self._correlation_id

        @correlation_id.setter
        def correlation_id(self, value: Optional[str]):
            self._correlation_id = value

        model_config = {
            # Allow arbitrary types (for complex domain objects)
            "arbitrary_types_allowed": True,
            # Validate on assignment
            "validate_assignment": True,
        }

        def to_dict(self) -> Dict[str, Any]:
            """Convert to dictionary."""
            return self.model_dump()

    class PydanticQuery(BaseModel, AbstractQuery):
        """
        Base class for Pydantic-validated queries.

        Similar to PydanticCommand but for read operations.
        """

        _query_id: Optional[str] = PrivateAttr(
            default_factory=lambda: str(import_uuid().uuid4())
        )
        _correlation_id: Optional[str] = PrivateAttr(default=None)

        @property
        def query_id(self) -> str:
            return self._query_id

        @property
        def correlation_id(self) -> Optional[str]:
            return self._correlation_id

        @correlation_id.setter
        def correlation_id(self, value: Optional[str]):
            self._correlation_id = value

        model_config = {"arbitrary_types_allowed": True}

        def to_dict(self) -> Dict[str, Any]:
            return self.model_dump()

    class PydanticValueObject(BaseModel):
        """
        Base class for Pydantic-based Value Objects.

        Traits:
        - Immutable (frozen)
        - Equality based on value (properties)
        - Validated on instantiation
        """

        model_config = {"frozen": True, "arbitrary_types_allowed": True}

        def __eq__(self, other) -> bool:
            if not isinstance(other, self.__class__):
                return False
            return self.model_dump() == other.model_dump()

        def __hash__(self) -> int:
            return hash(tuple(sorted(self.model_dump().items())))

    class PydanticEntity(BaseModel):
        """
        Base class for Pydantic-based Entities.

        Traits:
        - Mutable (unless configured otherwise)
        - Identity based on ID validation
        - Tracks version automatically
        - Includes audit fields (created_at, updated_at, created_by)
        - Supports soft delete (is_deleted, deleted_at)
        """

        id: Any = Field(default_factory=lambda: str(import_uuid().uuid4()))
        version: int = 0
        # Audit fields
        created_at: Any = Field(default_factory=lambda: PydanticEntity._get_time())
        updated_at: Any = None
        # Soft delete
        is_deleted: bool = False
        deleted_at: Any = None

        # Internal event tracking
        _domain_events: list = PrivateAttr(default_factory=list)

        model_config = {"validate_assignment": True, "arbitrary_types_allowed": True}

        @staticmethod
        def _get_time():
            from datetime import datetime, timezone

            return datetime.now(timezone.utc)

        def __eq__(self, other) -> bool:
            # Entities compared by ID
            return isinstance(other, PydanticEntity) and self.id == other.id

        def __hash__(self) -> int:
            return hash(self.id)

        def increment_version(self) -> None:
            """Increment version and update timestamp."""
            self.version += 1
            self.updated_at = PydanticEntity._get_time()

        def soft_delete(self) -> None:
            """Mark entity as deleted (soft delete)."""
            if self.is_deleted:
                return
            self.is_deleted = True
            self.deleted_at = PydanticEntity._get_time()
            self.increment_version()

        def restore(self) -> None:
            """Restore a soft-deleted entity."""
            if not self.is_deleted:
                return
            self.is_deleted = False
            self.deleted_at = None
            self.increment_version()

        def add_domain_event(self, event: Any) -> None:
            """
            Add a domain event to be dispatched.
            Complements the Modification pattern.
            """
            self._domain_events.append(event)
            self.increment_version()

        def clear_domain_events(self) -> list:
            """Clear and return all pending domain events."""
            events = self._domain_events.copy()
            self._domain_events.clear()
            return events

        def check_version(self, expected_version: int) -> None:
            """Verify that the entity's version matches expected version."""
            if self.version != expected_version:
                from ..exceptions import ConcurrencyError

                raise ConcurrencyError(expected=expected_version, actual=self.version)

    class PydanticDomainEvent(BaseModel, AbstractDomainEvent):
        """
        Base class for Pydantic-based Domain Events.

        Compatible with EventStore expectations (duck typing).
        """

        event_id: str = Field(default_factory=lambda: str(import_uuid().uuid4()))
        occurred_at: Any = Field(
            default_factory=lambda: PydanticDomainEvent._get_time()
        )
        correlation_id: Any = None
        causation_id: Any = None
        version: int = 1

        model_config = {"frozen": True, "arbitrary_types_allowed": True}

        def __init_subclass__(cls, **kwargs):
            """Auto-register event classes with EventTypeRegistry."""
            super().__init_subclass__(**kwargs)
            from ..event_registry import EventTypeRegistry

            EventTypeRegistry.register_class(cls)

        @staticmethod
        def _get_time():
            from datetime import datetime, timezone

            return datetime.now(timezone.utc)

        @property
        def event_type(self) -> str:
            return self.__class__.__name__

        @property
        def aggregate_type(self) -> str:
            return "Unknown"

        @property
        def aggregate_id(self) -> Any:
            raise NotImplementedError("Subclasses must implement aggregate_id")

        def to_dict(self) -> Dict[str, Any]:
            data = self.model_dump()
            # Ensure dates are isoformat for storage compatibility
            if hasattr(self.occurred_at, "isoformat"):
                data["occurred_at"] = self.occurred_at.isoformat()
            data["event_type"] = self.event_type
            data["aggregate_type"] = self.aggregate_type
            data["aggregate_id"] = self.aggregate_id
            return data

        @classmethod
        def from_dict(cls, data: Dict[str, Any]) -> "PydanticDomainEvent":
            """Reconstruct event from stored payload."""
            return cls.model_validate(data)

else:
    # Fallback for no Pydantic
    @dataclass
    class PydanticCommand:
        """PydanticCommand requires pydantic to be installed."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Pydantic is required. Install with: pip install py-cqrs-ddd-toolkit[pydantic]"
            )

    PydanticQuery = PydanticCommand
    PydanticValueObject = PydanticCommand
    PydanticEntity = PydanticCommand
    PydanticDomainEvent = PydanticCommand


def import_uuid():
    """Lazy import for uuid."""
    import uuid

    return uuid


# =============================================================================
# Pydantic-based Sagas
# =============================================================================

SagaStateT = TypeVar("SagaStateT", bound=BaseModel)

if HAS_PYDANTIC:

    class PydanticSaga(Saga[SagaStateT]):
        """
        Base class for Sagas using Pydantic for state management.

        Usage:
            class MyState(BaseModel):
                counter: int = 0

            class MySaga(PydanticSaga[MyState]):
                state_model = MyState

                async def on_Event(self, event):
                    self.state.counter += 1
                    # self.context.state is automatically sync'd via _sync_state
        """

        state_model: Type[SagaStateT]

        def _load_state(self):
            """Load state from context into Pydantic model."""
            # Use explicit state_model if provided (preferred for PydanticSaga)
            # otherwise fall back to base logic (which might be fragile with Generics)
            model_cls = getattr(self, "state_model", None)

            if model_cls:
                if self.context.state:
                    try:
                        # Validate and Load
                        self._state = model_cls(**self.context.state)
                    except Exception as e:
                        # Log and Re-raise (Corrupted State)
                        from ..saga import logger

                        logger.error(
                            f"Failed to load Pydantic state for {self.id}: {e}"
                        )
                        raise ValueError(f"Saga State Corruption: {e}") from e
                else:
                    # Initialize default
                    self._state = model_cls()
            else:
                super()._load_state()

        async def start(self, initial_data: Any) -> None:
            """Start the saga with initial data (supports Pydantic models)."""
            self.context.updated_at = self.context.updated_at  # touch

            if isinstance(initial_data, BaseModel):
                self._state = initial_data
            elif isinstance(initial_data, dict):
                model_cls = getattr(self, "state_model", None)
                if model_cls:
                    self._state = model_cls(**initial_data)
                else:
                    # Fallback to base behavior (generic or dict)
                    await super().start(initial_data)
                    return
            else:
                await super().start(initial_data)
                return

        def _sync_state(self):
            """Sync Pydantic state back to context.state dict."""
            if isinstance(self._state, BaseModel):
                self.context.state.clear()
                self.context.state.update(self._state.model_dump())
            else:
                super()._sync_state()

        # NOTE: We do not need to override handle_event because
        # the base Saga class calls _sync_state() after handler execution.

    class PydanticOutboxMessage(BaseModel, AbstractOutboxMessage):
        """
        Pydantic version of OutboxMessage for API use.
        """

        id: Any  # UUID
        occurred_at: Any  # datetime
        type: str
        topic: str
        payload: Dict[str, Any]
        correlation_id: Optional[str] = None
        status: str = "pending"
        retries: int = 0
        error: Optional[str] = None
        processed_at: Optional[Any] = None


else:
    # Fallback for no Pydantic
    @dataclass
    class PydanticSaga:
        """PydanticSaga requires pydantic to be installed."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Pydantic is required for PydanticSaga. Install with: pip install py-cqrs-ddd-toolkit[pydantic]"
            )

    @dataclass
    class PydanticOutboxMessage:
        """PydanticOutboxMessage requires pydantic to be installed."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Pydantic is required. Install with: pip install py-cqrs-ddd-toolkit[pydantic]"
            )


# =============================================================================
# Pydantic Validator Middleware
# =============================================================================

T = TypeVar("T")


class PydanticValidator(Generic[T]):
    """
    Base class for Pydantic-based validators with async context.

    Use when you need database lookups or other async operations
    during validation.

    Usage:
        class CreateUserValidator(PydanticValidator[CreateUser]):
            def __init__(self, user_repository):
                self.user_repository = user_repository

            async def init_context(self, cmd: CreateUser):
                # Async setup - check if email exists
                self.existing_user = await self.user_repository.find_by_email(cmd.email)

            def validate_email(self, email: str) -> str:
                if self.existing_user:
                    raise ValueError(f"Email '{email}' already registered")
                return email
    """

    def __init__(self):
        self._errors: Dict[str, list] = {}

    async def init_context(self, command: T) -> None:
        """
        Async initialization for validation context.

        Override to perform database lookups, fetch related data, etc.
        """
        pass

    async def validate(self, command: T) -> "ValidationResult":
        """
        Run full validation.

        1. Calls init_context() for async setup
        2. Runs all validate_* methods
        3. Returns ValidationResult with any errors
        """
        self._errors = {}

        # Async context initialization
        await self.init_context(command)

        # Run all validate_* methods
        for name in dir(self):
            if name.startswith("validate_") and callable(getattr(self, name)):
                field_name = name[9:]  # Remove 'validate_' prefix
                validator = getattr(self, name)
                field_value = getattr(command, field_name, None)

                try:
                    validator(field_value)
                except ValueError as e:
                    if field_name not in self._errors:
                        self._errors[field_name] = []
                    self._errors[field_name].append(str(e))

        return ValidationResult(errors=self._errors)


@dataclass
class ValidationResult:
    """Result of validation."""

    errors: Dict[str, list]

    def has_errors(self) -> bool:
        """Check if there are any validation errors."""
        return len(self.errors) > 0

    def is_valid(self) -> bool:
        """Check if validation passed."""
        return not self.has_errors()


# =============================================================================
# Response Models
# =============================================================================

if HAS_PYDANTIC:

    class CommandResult(BaseModel, Generic[T]):
        """Standard command result response payload."""

        success: bool = True
        result: Optional[T] = None
        errors: Dict[str, list] = Field(default_factory=dict)

    class QueryResult(BaseModel, Generic[T]):
        """Standard query result response payload."""

        data: T
        total: Optional[int] = None
        page: Optional[int] = None
        page_size: Optional[int] = None

    class PydanticCommandResponse(BaseModel, Generic[T]):
        """Pydantic-based version of CommandResponse envelope."""

        result: CommandResult[T]
        events: list = Field(default_factory=list)
        correlation_id: Optional[str] = None
        causation_id: Optional[str] = None

    class PydanticQueryResponse(BaseModel, Generic[T]):
        """Pydantic-based version of QueryResponse envelope."""

        result: QueryResult[T]
        correlation_id: Optional[str] = None
        causation_id: Optional[str] = None

else:

    @dataclass
    class CommandResult:
        success: bool = True
        result: Any = None
        errors: Dict[str, list] = None

    @dataclass
    class QueryResult:
        data: Any = None
        total: int = None
        page: int = None
        page_size: int = None
