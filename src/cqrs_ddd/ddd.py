"""DDD Building Blocks - Entity, AggregateRoot, ValueObject, DomainEvent, Modification."""

from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar, Generic, List, Optional, TYPE_CHECKING
import uuid

from .domain_event import DomainEventBase
from .exceptions import ConcurrencyError

if TYPE_CHECKING:
    from .persistence_dispatcher import PersistenceDispatcher

TEntity = TypeVar("TEntity")


def default_id_generator() -> str:
    """
    Generate a unique ID for entities.

    Returns:
        A UUID4 string.
    """
    return str(uuid.uuid4())


class Entity(ABC):
    """
    Base class for domain entities.

    Entities have identity and are compared by their ID, not their attributes.
    This base class includes standard fields for auditing and lifecycle management:
    - ID generation
    - Creation and update timestamps
    - Optimistic concurrency control (versioning)
    - Soft delete capabilities
    """

    def __init__(
        self,
        entity_id: Any = None,
        id_generator: Callable[[], Any] = default_id_generator,
        version: int = 0,
        # Audit fields
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        created_by: Optional[str] = None,
        # Soft delete
        is_deleted: bool = False,
        deleted_at: Optional[datetime] = None,
    ):
        """
        Initialize entity with ID, audit fields, and soft delete state.

        Args:
            entity_id: Explicit ID for the entity
            id_generator: Callable that returns a new ID (default: UUID4)
            version: Optimistic concurrency version (default: 0)
            created_at: When entity was created (auto-set if None)
            updated_at: When entity was last updated
            created_by: User who created the entity
            is_deleted: Soft delete flag
            deleted_at: When entity was soft-deleted
        """
        self._id = entity_id if entity_id is not None else id_generator()
        self._version = version
        self._created_at = created_at or datetime.now(timezone.utc)
        self._updated_at = updated_at
        self._created_by = created_by
        self._is_deleted = is_deleted
        self._deleted_at = deleted_at

    @property
    def id(self) -> Any:
        """Immutable entity ID."""
        return self._id

    @property
    def version(self) -> int:
        """Optimistic concurrency version."""
        return self._version

    @property
    def created_at(self) -> datetime:
        """When the entity was created."""
        return self._created_at

    @property
    def updated_at(self) -> Optional[datetime]:
        """When the entity was last updated."""
        return self._updated_at

    @property
    def created_by(self) -> Optional[str]:
        """User who created the entity."""
        return self._created_by

    @property
    def is_deleted(self) -> bool:
        """Whether the entity is soft-deleted."""
        return self._is_deleted

    @property
    def deleted_at(self) -> Optional[datetime]:
        """When the entity was soft-deleted."""
        return self._deleted_at

    def increment_version(self) -> None:
        """Increment the entity version and update timestamp (called on modification)."""
        self._version += 1
        self._updated_at = datetime.now(timezone.utc)

    def check_version(self, expected_version: int) -> None:
        """
        Verify that the entity's version matches expected version.

        Raises:
            ConcurrencyError: If versions mismatch
        """
        if self._version != expected_version:
            raise ConcurrencyError(expected=expected_version, actual=self._version)

    def soft_delete(self) -> None:
        """Mark entity as deleted (soft delete)."""
        if self._is_deleted:
            return
        self._is_deleted = True
        self._deleted_at = datetime.now(timezone.utc)
        self.increment_version()

    def restore(self) -> None:
        """Restore a soft-deleted entity."""
        if not self._is_deleted:
            return
        self._is_deleted = False
        self._deleted_at = None
        self.increment_version()

    def __eq__(self, other) -> bool:
        return isinstance(other, Entity) and self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class AggregateRoot(Entity):
    """
    Base class for aggregate roots.

    Aggregate roots are the gateways to their aggregates and must verify invariants.
    They are responsible for:
    - Maintaining consistency boundaries
    - Tracking domain events that occur within the aggregate
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._domain_events: List[Any] = []

    def add_domain_event(self, event: Any) -> None:
        """
        Add a domain event to be dispatched.

        Automatically increments the aggregate version, as adding an event
        implies a state change.
        """
        self._domain_events.append(event)
        self.increment_version()

    def clear_domain_events(self) -> List[Any]:
        """Clear and return all pending domain events."""
        events = self._domain_events.copy()
        self._domain_events.clear()
        return events


class ValueObject(ABC):
    """
    Base class for value objects.

    Value objects have no identity and are defined by their attributes.
    They should be immutable. Equality checks compare all fields.

    Usage:
        @dataclass(frozen=True)
        class MyValueObject(ValueObject):
            field1: str
            field2: int
    """

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.__dict__.items())))


@dataclass
class DomainEvent(DomainEventBase):
    """Base class for Domain Events."""
    pass


@dataclass
class Modification(Generic[TEntity]):
    """
    Represents the result of a domain operation (mostly Write).

    It wraps the modified entity and any domain events produced during the operation.
    This ensures that both state changes and events are propagated together to the
    persistence and messaging layers.
    """

    entity: TEntity
    events: List[Any] = field(default_factory=list)

    @property
    def entity_name(self) -> str:
        """Derive entity name from entity's class."""
        return type(self.entity).__name__

    def get_domain_events(self) -> List[Any]:
        """Return copy of domain events."""
        return self.events.copy()

    async def apply(
        self, dispatcher: "PersistenceDispatcher", unit_of_work: Any = None
    ) -> Any:
        """
        Apply this modification via the dispatcher.

        Args:
            dispatcher: The persistence dispatcher
            unit_of_work: Optional existing UoW

        Returns:
            Affected entity ID(s)
        """
        return await dispatcher.apply(self, unit_of_work)
