"""Event Store Foundation - Event persistence implementation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import logging

from .domain_event import DomainEventBase, generate_correlation_id
from .protocols import EventStore, UndoService  # noqa: F401

logger = logging.getLogger(__name__)

# =============================================================================
# Event Store Protocol Data
# =============================================================================


@dataclass
class StoredEvent:
    """
    Event as stored in the event store.

    This flat structure is suitable for database persistence.
    It contains all necessary metadata for event sourcing and audit logs.

    Fields:
        id: Sequence ID (global order)
        event_id: Unique UUID for the event
        aggregate_type: Type of the aggregate (e.g. "Order")
        aggregate_id: ID of the aggregate instance
        event_type: Class name of the event
        event_version: Schema version of the event
        occurred_at: Timestamp when event happened
        user_id: ID of variable user who triggered the command
        correlation_id: ID linking cascading events
        causation_id: ID of the command/event that caused this
        payload: The actual event data (serialized)
        aggregate_version: Version of the aggregate AFTER this event
        is_undone: Flag for soft-undo
        undone_at: Timestamp when undone
        undone_by: User who performed undo
        undo_event_id: ID of the undo event
    """

    id: int
    event_id: str
    aggregate_type: str
    aggregate_id: Any
    event_type: str
    event_version: int
    occurred_at: datetime
    user_id: Optional[str]
    correlation_id: Optional[str]
    causation_id: Optional[str]
    payload: Dict[str, Any]
    aggregate_version: int
    is_undone: bool = False
    undone_at: Optional[datetime] = None
    undone_by: Optional[str] = None
    undo_event_id: Optional[str] = None



# =============================================================================
# Event Store Middleware
# =============================================================================


class EventStoreMiddleware:
    """
    Middleware that persists domain events from command responses.

    This middleware:
    1. Generates a correlation ID for the command
    2. Executes the handler
    3. Persists all domain events from the response
    4. Enriches events with user/correlation info

    Usage:
        @middleware.apply(EventStoreMiddleware, event_store=my_store)
        class UpdateElementHandler(CommandHandler):
            ...
    """

    def __init__(self, event_store: EventStore, get_user_id: Optional[callable] = None):
        """
        Initialize the middleware.

        Args:
            event_store: The event store to persist to
            get_user_id: Optional callable to get current user ID
        """
        self.event_store = event_store
        self.get_user_id = get_user_id

    def apply(self, handler_func, command):
        """Wrap handler to persist events."""

        async def wrapped_handler(*args, **kwargs):
            # Generate correlation ID for this command
            correlation_id = generate_correlation_id()

            # Get current user
            user_id = self.get_user_id() if self.get_user_id else None

            # Execute handler
            result = await handler_func(*args, **kwargs)

            # Persist events if present
            if hasattr(result, "events") and result.events:
                events_to_persist = []

                for event in result.events:
                    # Enrich event with context
                    if hasattr(event, "correlation_id") and not event.correlation_id:
                        event.correlation_id = correlation_id
                    if hasattr(event, "user_id") and not event.user_id:
                        event.user_id = user_id

                    events_to_persist.append(event)

                # Persist all events
                await self.event_store.append_batch(
                    events=events_to_persist, correlation_id=correlation_id
                )

            return result

        return wrapped_handler


# =============================================================================
# In-Memory Event Store (for testing)
# =============================================================================


class InMemoryEventStore(EventStore):
    """
    In-memory event store implementation for testing.

    NOT for production use - events are lost on restart.
    """

    def __init__(self):
        self._events: List[StoredEvent] = []
        self._versions: Dict[
            tuple, int
        ] = {}  # (aggregate_type, aggregate_id) -> version
        self._next_id = 1

    async def append(
        self, event: DomainEventBase, expected_version: Optional[int] = None
    ) -> StoredEvent:
        """
        Append a single event to the store.

        Args:
            event: The domain event to persist.
            expected_version: Optimistic concurrency check.
                              If set, must match current aggregate version.

        Raises:
            CQRSDDDError: If expected_version mismatch.
        """
        key = (event.aggregate_type, event.aggregate_id)
        current_version = self._versions.get(key, 0)

        if expected_version is not None and current_version != expected_version:
            from .exceptions import CQRSDDDError

            raise CQRSDDDError(
                f"Concurrency error: expected version {expected_version}, "
                f"but current version is {current_version}"
            )

        new_version = current_version + 1
        self._versions[key] = new_version

        stored = StoredEvent(
            id=self._next_id,
            event_id=event.event_id,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            event_type=event.event_type,
            event_version=event.version,
            occurred_at=event.occurred_at,
            user_id=event.user_id,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            payload=event.to_dict(),
            aggregate_version=new_version,
        )

        self._events.append(stored)
        self._next_id += 1

        return stored

    async def append_batch(
        self, events: List[DomainEventBase], correlation_id: Optional[str] = None
    ) -> List[StoredEvent]:
        """
        Append multiple events in a single batch.

        Enriches all events with the provided correlation_id before appending.
        """
        from .domain_event import enrich_event_metadata

        results = []
        for event in events:
            event = enrich_event_metadata(event, correlation_id=correlation_id)
            stored = await self.append(event)
            results.append(stored)
        return results

    async def get_events(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None,
    ) -> List[StoredEvent]:
        results = [
            e
            for e in self._events
            if e.aggregate_type == aggregate_type
            and e.aggregate_id == aggregate_id
            and not e.is_undone
        ]

        logger.debug(
            f"EventStore.get_events: Found {len(results)} events for {aggregate_type}:{aggregate_id}"
        )
        if len(self._events) > 0 and len(results) == 0:
            # Log potential mismatches for debugging
            for e in self._events:
                logger.debug(
                    f"Event in store: {e.aggregate_type}:{e.aggregate_id} (ID: {e.id}, Type: {e.event_type})"
                )

        if from_version is not None:
            results = [e for e in results if e.aggregate_version >= from_version]
        if to_version is not None:
            results = [e for e in results if e.aggregate_version <= to_version]

        return sorted(results, key=lambda e: e.aggregate_version)

    async def get_events_by_correlation(self, correlation_id: str) -> List[StoredEvent]:
        return [e for e in self._events if e.correlation_id == correlation_id]

    async def get_latest_events(
        self, aggregate_type: str, aggregate_id: Any, count: int = 10
    ) -> List[StoredEvent]:
        events = await self.get_events(aggregate_type, aggregate_id)
        return events[-count:] if len(events) > count else events

    async def get_event(self, event_id: str) -> Optional[StoredEvent]:
        """Get a specific event by its event_id."""
        for event in self._events:
            if event.event_id == event_id:
                return event
        return None

    async def get_undone_events(
        self, aggregate_type: str, aggregate_id: Any, count: int = 10
    ) -> List[StoredEvent]:
        """Get undone events for redo stack."""
        results = [
            e
            for e in self._events
            if e.aggregate_type == aggregate_type
            and e.aggregate_id == aggregate_id
            and e.is_undone
        ]
        # Return most recently undone first
        results = sorted(
            results, key=lambda e: e.undone_at or e.occurred_at, reverse=True
        )
        return results[:count]

    async def mark_as_undone(
        self, event_id: str, undone_by: str, undo_event_id: Optional[str] = None
    ) -> None:
        for event in self._events:
            if event.event_id == event_id:
                event.is_undone = True
                event.undone_at = datetime.now(timezone.utc)
                event.undone_by = undone_by
                event.undo_event_id = undo_event_id
                break

    async def mark_as_redone(self, event_id: str) -> None:
        """Clear the is_undone flag (for redo operations)."""
        for event in self._events:
            if event.event_id == event_id:
                event.is_undone = False
                event.undone_at = None
                event.undone_by = None
                event.undo_event_id = None
                break

    async def get_current_version(self, aggregate_type: str, aggregate_id: Any) -> int:
        return self._versions.get((aggregate_type, aggregate_id), 0)

    def clear(self) -> None:
        """Clear all events (for testing)."""
        self._events.clear()
        self._versions.clear()
        self._next_id = 1
