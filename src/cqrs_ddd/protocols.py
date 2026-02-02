"""Protocol definitions for CQRS/DDD toolkit.

All protocols use @runtime_checkable for structural typing support.
"""
from typing import Protocol, TypeVar, Generic, runtime_checkable, Any, Optional, List, Dict, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .domain_event import DomainEventBase

T = TypeVar('T')
TResult = TypeVar('TResult')


@runtime_checkable
class UnitOfWork(Protocol):
    """
    Unit of Work protocol for transaction management.
    
    Implementations should handle transaction lifecycle:
    - Begin transaction on __aenter__
    - Commit on successful __aexit__
    - Rollback on exception in __aexit__
    """
    
    async def __aenter__(self) -> 'UnitOfWork':
        """Start the unit of work scope."""
        ...
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """End the unit of work scope, commit or rollback."""
        ...
    
    async def commit(self) -> None:
        """Explicitly commit the transaction."""
        ...
    
    async def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        ...


@runtime_checkable
class EventDispatcher(Protocol):
    """
    Event dispatcher protocol for domain events.
    
    Supports two types of handlers:
    - Priority: Awaited synchronously before command returns
    - Background: Fire-and-forget after transaction commit
    """
    
    def register(
        self, 
        event_type: type, 
        handler: object, 
        priority: bool = False
    ) -> None:
        """Register an event handler."""
        ...
    
    async def dispatch_priority(self, event: object) -> None:
        """Dispatch to priority handlers (sync, in-transaction)."""
        ...
    
    async def dispatch_background(self, event: object) -> None:
        """Dispatch to background handlers (async, post-commit)."""
        ...


@runtime_checkable
class CacheService(Protocol):
    """
    Cache service protocol for caching operations.
    
    Implementations can use Redis, Memcached, or in-memory storage.
    """
    
    async def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        ...
    
    async def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set a value in cache with optional TTL."""
        ...
    
    async def delete(self, key: str) -> None:
        """Delete a value from cache."""
        ...
    
    async def delete_batch(self, keys: List[str]) -> None:
        """Delete multiple values from cache."""
        ...
    
    async def get_batch(self, keys: List[str]) -> List[Optional[Any]]:
        """Get multiple values from cache."""
        ...
    
    async def set_batch(
        self, 
        entries: List[dict], 
        ttl: int = None
    ) -> None:
        """Set multiple values in cache. Each entry has 'cache_key' and 'value'."""
        ...
    
    def generate_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate a cache key for entity caching."""
        ...
    
    def generate_query_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate a cache key for query result caching."""
        ...


@runtime_checkable
class LockStrategy(Protocol):
    """
    Lock strategy protocol for distributed locking.
    
    Implementations can use Redis, database locks, or in-memory for testing.
    Used by ThreadSafetyMiddleware to prevent concurrent modifications.
    """
    
    async def acquire(
        self, 
        entity_type: str, 
        entity_id_or_ids: Any,
        timeout: int = 30
    ) -> str:
        """
        Acquire a lock for the given entity/entities.
        
        For multiple IDs, implementations should sort them before locking
        to prevent deadlocks.
        
        Args:
            entity_type: Type of entity (e.g., 'Layer', 'Element')
            entity_id_or_ids: Single ID or list of IDs to lock
            timeout: Lock timeout in seconds
        
        Returns:
            Lock token for release
        """
        ...
    
    async def release(
        self, 
        entity_type: str, 
        entity_id_or_ids: Any,
        token: str
    ) -> None:
        """
        Release a previously acquired lock.
        
        Args:
            entity_type: Type of entity
            entity_id_or_ids: Single ID or list of IDs to unlock
            token: Lock token from acquire()
        """
        ...


@runtime_checkable
class StorageService(Protocol):
    """
    Storage service protocol for file/blob storage.
    
    Abstracts filesystem operations to allow swapping implementations
    (Local, S3, Azure Blob, etc.). All methods are async.
    """
    
    async def save(self, path: str, content: bytes, overwrite: bool = False) -> None:
        """Save content to path (atomic)."""
        ...
        
    async def read(self, path: str) -> Optional[bytes]:
        """Read content from path. Returns None if not found."""
        ...
        
    async def delete(self, path: str) -> None:
        """Delete file at path. Should not raise if file missing."""
        ...
        
    async def exists(self, path: str) -> bool:
        """Check if file exists."""
        ...
        
    async def list(self, path: str) -> List[str]:
        """List files in directory."""
        ...


# =============================================================================
# Messaging Protocols
# =============================================================================

@runtime_checkable
class MessagePublisher(Protocol):
    """
    Protocol for publishing messages to a transport.
    """
    
    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """
        Publish a message to a topic/exchange.
        
        Args:
            topic: The routing key or topic name
            message: The message object or payload
            **kwargs: Additional metadata (correlation_id, user_id, etc.)
        """
        ...


@runtime_checkable
class MessageConsumer(Protocol):
    """
    Protocol for reading messages from a transport.
    """
    
    async def subscribe(self, topic: str, handler: Any, queue_name: Optional[str] = None) -> None:
        """
        Subscribe to a topic/queue.
        
        Args:
            topic: The routing key or queue name to bind
            handler: Async callable to process received messages
            queue_name: Optional explicit queue name for group consumption
        """
        ...


@runtime_checkable
class MessageBroker(MessagePublisher, MessageConsumer, Protocol):
    """
    Message broker protocol for asynchronous event communication.
    
    Combines both publishing and subscribing capabilities.
    """
    pass


# =============================================================================
# Event Store Protocols
# =============================================================================

@runtime_checkable
class EventStore(Protocol):
    """
    Protocol for persisting and retrieving domain events.
    """
    
    async def append(
        self, 
        event: Any,
        expected_version: Optional[int] = None
    ) -> Any:
        """Append an event to the store."""
        ...
    
    async def append_batch(
        self,
        events: List[Any],
        correlation_id: Optional[str] = None
    ) -> List[Any]:
        """Append multiple events atomically."""
        ...
    
    async def get_events(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[Any]:
        """Get events for a specific aggregate."""
        ...
    
    async def get_events_by_correlation(
        self,
        correlation_id: str
    ) -> List[Any]:
        """Get events by correlation ID."""
        ...
    
    async def get_latest_events(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        count: int = 10
    ) -> List[Any]:
        """Get most recent events for an aggregate."""
        ...
    
    async def get_event(self, event_id: str) -> Optional[Any]:
        """Get a specific event by ID."""
        ...
    
    async def get_undone_events(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        count: int = 10
    ) -> List[Any]:
        """Get undone events for redo stack."""
        ...
    
    async def mark_as_undone(
        self,
        event_id: str,
        undone_by: str,
        undo_event_id: Optional[str] = None
    ) -> None:
        """Mark an event as undone."""
        ...
    
    async def mark_as_redone(self, event_id: str) -> None:
        """Clear the is_undone flag."""
        ...
    
    async def get_current_version(
        self,
        aggregate_type: str,
        aggregate_id: Any
    ) -> int:
        """Get current aggregate version."""
        ...


@runtime_checkable
class UndoExecutor(Protocol[T]):
    """Protocol for undoing a specific event type."""
    
    @property
    def event_type(self) -> str:
        """The event type this executor handles."""
        ...
    
    async def can_undo(self, event: T) -> bool:
        """Check if the event can still be undone."""
        ...
    
    async def undo(self, event: T) -> List['DomainEventBase']:
        """Execute undo, returning compensating events."""
        ...
    
    async def redo(self, event: T, undo_event: Any) -> List['DomainEventBase']:
        """Re-apply a previously undone event."""
        ...


@runtime_checkable
class UndoExecutorRegistry(Protocol):
    """Protocol for undo executor registry."""
    
    def register(self, executor: UndoExecutor) -> None:
        ...
    
    def get(self, event_type: str) -> Optional[UndoExecutor]:
        ...
    
    def has_executor(self, event_type: str) -> bool:
        ...


@runtime_checkable
class UndoService(Protocol):
    """Protocol for undo/redo operations."""
    
    async def get_undo_stack(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        depth: int = 10
    ) -> List[Any]:
        """Get list of undoable actions."""
        ...
    
    async def undo(
        self,
        event_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Any:
        """Execute undo operation."""
        ...
    
    async def redo(
        self,
        undo_event_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Any:
        """Redo a previously undone action."""
        ...


# =============================================================================
# Outbox Protocols
# =============================================================================

@runtime_checkable
class OutboxProcessor(Protocol):
    """
    Protocol for outbox background workers.
    
    Implementations should handle polling and publishing 
    pending messages from the OutboxStorage.
    """
    
    async def start(self) -> None:
        """Start the background processing."""
        ...
    
    async def stop(self) -> None:
        """Gracefully stop the background processing."""
        ...


@runtime_checkable
class OutboxStorage(Protocol):
    """Protocol for persisting outbox messages."""
    
    async def save(self, message: Any) -> None:
        """Save a new message to the outbox."""
        ...
    
    async def get_pending(self, batch_size: int = 50) -> List[Any]:
        """Get pending messages ordered by occurrence time."""
        ...
    
    async def mark_published(self, message_id: Any) -> None:
        """Mark message as successfully published."""
        ...
    
    async def mark_failed(self, message_id: Any, error: str) -> None:
        """Mark message as failed and increment retries."""
        ...


@runtime_checkable
class EventConsumer(Protocol):
    """
    Protocol for event consumers that bridge a broker and a dispatcher.
    
    Implementations should subscribe to topics on a MessageConsumer,
    hydrate incoming payloads into DomainEvents, and dispatch them.
    """
    
    async def start(self) -> None:
        """Start consuming events."""
        ...
    
    async def stop(self) -> None:
        """Stop consuming events."""
        ...


# =============================================================================
# Saga Protocols
# =============================================================================

@runtime_checkable
class SagaRepository(Protocol):
    """Protocol for persisting saga state."""
    
    async def save(self, context: Any) -> None:
        ...
    
    async def load(self, saga_id: str) -> Optional[Any]:
        ...
    
    async def find_by_correlation_id(self, correlation_id: str, saga_type: str) -> Optional[Any]:
        ...

    async def find_stalled_sagas(self, limit: int = 10) -> List[Any]:
        """Find sagas that are stalled (is_stalled=True)."""
        ...

    async def find_suspended_sagas(self, limit: int = 10) -> List[Any]:
        """Find sagas that are suspended (is_suspended=True)."""
        ...

    async def find_expired_suspended_sagas(self, limit: int = 10) -> List[Any]:
        """Find sagas that are suspended and have timed out."""
        ...


@runtime_checkable
class SagaRegistry(Protocol):
    """Protocol for saga registry."""
    
    def get_sagas_for_event(self, event_type: type) -> List[type]:
        """Get all saga classes registered for an event type."""
        ...
    
    def get_saga_type(self, name: str) -> Optional[type]:
        """Get saga class by name."""
        ...


# =============================================================================
# Middleware Protocols
# =============================================================================

@runtime_checkable
class Middleware(Protocol):
    """
    Protocol for middleware.
    
    Middleware wraps handler execution to add cross-cutting concerns.
    """
    
    def apply(self, handler_func: Callable, message: Any) -> Callable:
        """
        Wrap the handler function with middleware logic.
        
        Args:
            handler_func: The next handler in the chain
            message: The command or query being processed
        
        Returns:
            Wrapped handler function
        """
        ...


@runtime_checkable
class AttributeInjector(Protocol):
    """
    Protocol for attribute injectors.
    
    Injectors compute derived values to be injected into commands.
    """
    
    async def compute(self, command: Any) -> Any:
        """Compute the value to inject."""
        ...
@runtime_checkable
class Validator(Protocol[T]):
    """
    Validator protocol for commands and queries.
    """
    
    async def validate(self, message: T) -> Any:
        """
        Validate the message.
        
        Args:
            message: The command or query to validate
            
        Returns:
            Validation result (implementation specific, usually a result object or raises exception)
        """
        ...
