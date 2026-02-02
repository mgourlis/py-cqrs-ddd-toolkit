"""
# py-cqrs-ddd-toolkit

A framework-agnostic Python library for building CQRS and DDD applications.

## Core Components

### CQRS
- `Command`, `Query` - Message types
- `CommandHandler`, `QueryHandler` - Handler base classes
- `Mediator` - Routes messages to handlers
- `EventDispatcher` - Handles domain events

### DDD Building Blocks
- `Entity`, `AggregateRoot` - Domain entities
- `ValueObject` - Immutable value types
- `DomainEvent` - Event base class
- `Modification` - Domain operation result

### Persistence
- `EntityPersistence` - Write operations
- `EntityRetrievalPersistence` - Read operations
- `QueryPersistence`, `SearchQueryPersistence` - Query operations
- `UnitOfWork` - Transaction protocol

### Middleware
- `ValidatorMiddleware` - Input validation
- `AuthorizationMiddleware` - Permission checks
- `LoggingMiddleware` - Request logging

### Framework Integrations
- `contrib.fastapi` - FastAPI integration
- `contrib.django` - Django integration
- `contrib.pydantic` - Pydantic validation

### Backends
- `backends.sqlalchemy` - SQLAlchemy UoW and persistence
- `backends.redis_cache` - Redis caching
- `backends.memory_cache` - In-memory cache (testing)
"""

from .core import (
    Command,
    Query,
    CommandHandler,
    QueryHandler,
    EventHandler,
    CommandResponse,
    QueryResponse,
)
from .mediator import Mediator
from .dispatcher import EventDispatcher

from .middleware import (
    Middleware,
    middleware,
    ValidatorMiddleware,
    AuthorizationMiddleware,
    LoggingMiddleware,
    ThreadSafetyMiddleware,
    AttributeInjector,
    AttributeInjectionMiddleware,
)
from .ddd import (
    Entity,
    AggregateRoot,
    ValueObject,
    DomainEvent,
    Modification,
)
from .persistence_dispatcher import (
    PersistenceDispatcher,
    OperationPersistence,
    QueryPersistence,
    RetrievalPersistence,
    list_modification_handlers,
    list_query_handlers,
    list_retrieval_handlers,
)
from .protocols import UnitOfWork, CacheService, LockStrategy
from .domain_event import (
    DomainEventBase,
    generate_event_id,
    generate_correlation_id,
)
from .event_store import (
    StoredEvent,
    EventStore,
    UndoService,
    EventStoreMiddleware,
    InMemoryEventStore,
)
from .undo import (
    UndoExecutor,
    UndoExecutorRegistry,
    UndoableAction,
    UndoResult,
    RedoResult,
)
from .event_registry import EventTypeRegistry

__version__ = "0.1.0"

__all__ = [
    # Core CQRS
    "Command",
    "Query",
    "CommandHandler",
    "QueryHandler",
    "EventHandler",
    "CommandResponse",
    "QueryResponse",
    "Mediator",
    "EventDispatcher",
    # Middleware
    "Middleware",
    "middleware",
    "ValidatorMiddleware",
    "AuthorizationMiddleware",
    "LoggingMiddleware",
    "ThreadSafetyMiddleware",
    "AttributeInjector",
    "AttributeInjectionMiddleware",
    # DDD
    "Entity",
    "AggregateRoot",
    "ValueObject",
    "DomainEvent",
    "Modification",
    # Persistence
    "EntityPersistence",
    "EntityModification",
    "EntityRetrievalPersistence",
    "QueryPersistence",
    "SearchQueryPersistence",
    # Persistence Dispatcher
    "PersistenceDispatcher",
    "OperationPersistence",
    "QueryPersistence",
    "RetrievalPersistence",
    "list_modification_handlers",
    "list_query_handlers",
    "list_retrieval_handlers",
    # Protocols
    "UnitOfWork",
    "CacheService",
    "LockStrategy",
    # Event Store
    "DomainEventBase",
    "StoredEvent",
    "EventStore",
    "UndoExecutor",
    "UndoExecutorRegistry",
    "UndoService",
    "UndoableAction",
    "UndoResult",
    "RedoResult",
    "EventStoreMiddleware",
    "InMemoryEventStore",
    "generate_event_id",
    "generate_correlation_id",
    "EventTypeRegistry",
]
