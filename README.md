# cqrs-ddd-toolkit: Analytical Architecture & Usage Guide

A high-performance, framework-agnostic toolkit for building complex systems using **CQRS (Command Query Responsibility Segregation)**, **DDD (Domain-Driven Design)**, and the **Saga Pattern**.

Designed for scalability, auditability, and clear separation of concerns, this toolkit provides the backbone for systems that require strong consistency, transactional integrity, and stateful orchestrations.

---

## Architecture Overview

The toolkit enforces a strict separation between layers, ensuring that domain logic remains pure while infrastructure concerns (persistence, messaging) are handled via decoupled adapters.

```mermaid
graph TD
    subgraph Presentation
        API[FastAPI / Django]
    end

    subgraph "Application Layer"
        M[Mediator]
        CH[Command Handlers]
        QH[Query Handlers]
        EH[Event Handlers]
    end

    subgraph "Saga Orchestration"
        SM[Saga Manager]
        S[Saga Instance]
        SR[Saga Repository]
    end

    subgraph "Domain Layer"
        AR[Aggregate Roots]
        E[Entities]
        VO[Value Objects]
        DE[Domain Events]
    end

    subgraph "Infrastructure Layer"
        PD[Persistence Dispatcher]
        OS[Outbox Service]
        ES[Event Store]
    end

    API -->|Sends| M
    M -->|Dispatches| CH
    M -->|Dispatches| QH
    CH -->|Mutates| AR
    AR -->|Produces| DE
    DE -->|Triggers| EH
    DE -->|Triggers| SM
    SM -->|Loads/Saves| SR
    SM -->|Coordinates| S
    S -->|Dispatches| M
    DE -->|Persisted to| OS
    CH -->|Operations| PD
```

---

## Domain-Driven Design (DDD)

The toolkit provides base classes that enforce DDD best practices while automating boilerplate like audit logging and concurrency control.

### 1. Entities & Aggregate Roots
`AggregateRoot` (which inherits from `Entity`) is the primary unit of consistency.

*   **Identity-Based**: Compared by `id` (UUID4 by default).
*   **Optimistic Concurrency**: Every state change increments a `version` field. The `PersistenceDispatcher` checks this version during save to prevent stale writes.
*   **Auditability**: Built-in `created_at`, `updated_at`, `created_by` fields.
*   **Lifecycle Management**: Native support for **Soft Delete** (`soft_delete`, `restore`).

```python
from cqrs_ddd.ddd import AggregateRoot, DomainEvent

class Product(AggregateRoot):
    def update_price(self, new_price: float):
        # Business logic / Invariant checks here
        if new_price < 0:
            raise ValueError("Price cannot be negative")

        self.price = new_price
        # Tracking side effects
        self.add_domain_event(ProductPriceChanged(id=self.id, price=new_price))
```

### 2. Value Objects
Value Objects are immutable descriptors of the domain with no identity. They are defined by their attributes.

*   **Immutable**: Always use with `@dataclass(frozen=True)`.
*   **Equality**: Comparing two Value Objects checks every field, not just an ID.

```python
from cqrs_ddd.ddd import ValueObject
from dataclasses import dataclass

@dataclass(frozen=True)
class Money(ValueObject):
    amount: float
    currency: str = "USD"
```

### 3. The Modification Pattern
A `Modification` is a container that pairs a modified entity with the events it produced. This ensures that the state change and its side effects are never separated.

*   **Atomic Persistence**: The `PersistenceDispatcher` uses the `Modification` to save the entity AND trigger event handlers/outbox publishing in a single transaction.

```python
from cqrs_ddd.ddd import Modification

# Logic inside a Command Handler
product = await repository.get(cmd.id)
product.update_price(cmd.price)

return Modification(
    entity=product,
    events=product.clear_domain_events()
)
```

### 4. Domain Events
Domain Events represent something that has happened in the past. They are the primary mechanism for decoupling and traceability.

*   **Standardized Metadata**: Every event automatically includes:
    *   `event_id`: Unique tracking ID.
    *   `occurred_at`: UTC timestamp.
    *   `correlation_id`: Groups related events from a single operation.
    *   `causation_id`: Tracks chains of events (Event A caused Event B).
*   **Auto-Registration**: Inheriting from `DomainEventBase` automatically registers the event in the `EventTypeRegistry`, enabling zero-config hydration.
*   **Serialization**: Built-in support for `to_dict()` and `from_dict()`, ensuring seamless storage in event stores or outboxes.

```python
from cqrs_ddd.domain_event import DomainEventBase

class ProductPriceChanged(DomainEventBase):
    product_id: str
    new_price: float

    @property
    def aggregate_id(self) -> str:
        return self.product_id
```

---

### 5. Aggregate Design & Boundaries

A common question is how to handle complex hierarchies or relations between multiple Aggregate Roots.

*   **Internal Entities**: An Aggregate Root can contain a collection of `Entity` objects. These entities have an ID that is unique **within the aggregate**, but they are always accessed and modified through the Root.
*   **External References**: When an Aggregate Root needs to refer to another, **always use the ID**, never a direct object reference. This maintains a clear consistency boundary and prevents transactional coupling.

```python
class Order(AggregateRoot):
    created_by_id: str  # Reference by ID to User AR
    items: List[OrderItem]  # Internal Entity collection

    def add_item(self, product_id: str, quantity: int):
        # Items are managed by the Order Root
        self.items.append(OrderItem(product_id=product_id, quantity=quantity))
        self.add_domain_event(ItemAddedToOrder(id=self.id, product_id=product_id))
```

---

## Application Core

The Application Layer coordinates domain objects to perform system tasks. The toolkit uses a class-based, auto-registering pattern to minimize boilerplate.

### 1. Commands & Queries
Messages are defined as `dataclass` objects. They are the entry points for all system operations.

*   **Commands**: Imperative (e.g., `RegisterUser`). Intent to mutate state.
*   **Queries**: Informational (e.g., `GetActiveUsers`). No side effects.
*   **Correlation Tracking**: Every message automatically generates a unique ID and supports `correlation_id` propagation for distributed tracing.

```python
from cqrs_ddd.core import Command, Query

class UpdateBio(Command):
    user_id: str
    new_bio: str

class GetUserBio(Query):
    user_id: str
```

### 2. Standardized Responses
To ensure consistency across the API and middleware, all handlers must return wrapped response objects.

*   **`CommandResponse[TResult]`**: Wraps the result and a list of `events` to be dispatched.
*   **`QueryResponse[TResult]`**: Wraps the read-only result.

```python
from cqrs_ddd.core import CommandResponse

# Handler return type
return CommandResponse(result=True, events=[UserBioUpdated(...)])
```

### 3. Auto-Registering Handlers
The toolkit uses Python type hints for **Dependency Discovery**. You don't need to manually link commands to handlers; inheriting from the base classes and providing a type hint for the first argument of `handle()` is enough.

*   **Command/Query Handlers**: Handled by exactly one handler.
*   **Event Handlers**: Can handle multiple events (using `Union`) and support `is_priority=True` for synchronous execution within the same transaction.

```python
from cqrs_ddd.core import CommandHandler, QueryHandler, CommandResponse, QueryResponse

# Command Example
class UpdateBioHandler(CommandHandler):
    async def handle(self, cmd: UpdateBio) -> CommandResponse[bool]:
        # Logic here...
        return CommandResponse(result=True)

# Query Example
class GetUserBioHandler(QueryHandler):
    async def handle(self, query: GetUserBio) -> QueryResponse[str]:
        # Logic here...
        return QueryResponse(result="User bio content")

# Event Example (Background/Asynchronous)
class WelcomeEmailHandler(EventHandler):
    async def handle(self, event: UserCreated):
        # Sends email in the background
        pass

# Event Example (Priority/Synchronous)
class AuditLogHandler(EventHandler):
    is_priority = True  # Runs in the same transaction

    async def handle(self, event: Union[UserCreated, BioUpdated]):
        # Handles multiple event types
        pass
```

### 4. Message Registry & Discovery
The toolkit maintains an internal registry (`handler_registry.py`) that acts as the "brain" for the Mediator.

*   **Explicit Mapping**: Maps every Command and Query to exactly one handler class.
*   **Dual-Queue Events**: Manages two separate queues for events:
    *   **Priority**: Handlers registered with `is_priority=True`. These run synchronously.
    *   **Background**: Standard handlers for asynchronous side effects.
*   **Introspection**: Use `get_registered_handlers()` to get a snapshot of all active mappings for debugging or system-wide auditing.

---

## Core Mechanics

### 1. Mediator Pattern (The Orchestrator)
The `Mediator` acts as the central hub for all message-based communication. It manages the lifecycle of commands and queries, ensuring consistency through middleware and transaction management.

#### Key Features
*   **Unit of Work (UoW)**: The Mediator uses `contextvars` to manage transactional scopes.
    *   **Root Commands**: Start a new transaction.
    *   **Nested Commands**: Automatically detect and reuse the existing UoW, enabling shared transactions across aggregate boundaries.
*   **Dual-Queue Event Propagation**: Every `CommandResponse` can contain events which the Mediator automatically enriches (with correlation IDs) and dispatches:
    *   **Priority Events**: Dispatched synchronously within the transaction.
    *   **Background Events**: Dispatched asynchronously after a successful commit.
*   **Zero-Config Discovery**: Automatically resolves handlers using the registry. If a handler has dependencies, it can be wired to a **Dependency Injection (DI)** container.

#### Configuration Example
```python
from cqrs_ddd.mediator import Mediator

# Initializing with a UoW factory and Event Dispatcher
mediator = Mediator(
    uow_factory=lambda: SQLAlchemyUoW(session_factory),
    event_dispatcher=dramatiq_event_dispatcher
)

# Usage
result = await mediator.send(CreateProduct(name="Coffee", price=5.0))
```
### 2. Middlewares & Pipelines
The toolkit implements a robust pipeline pattern (Chain of Responsibility) through a decorator-based middleware system. This allows you to attach cross-cutting concerns to any `CommandHandler` or `QueryHandler` without polluting business logic.

#### The "Onion" Model (Execution Order)
Middlewares are applied in a **stack-like** (Last-In, First-Out) fashion. When you decorate a handler, the Mediator builds an execution chain where:
1.  The **outermost** decorator executes first (initial entry).
2.  It calls `next()` to pass control to the next middleware.
3.  The **innermost** decorator executes last, immediately before calling the handler.
4.  Results then "bubble back up" through the chain.

#### Standard Middlewares & Decorators
The global `middleware` registry provides a declarative API for the most common needs:

*   **`@middleware.log(level="info")`**: Logs start, successful completion (with duration), and failures (with tracebacks). Automatically includes the `correlation_id` in logs.
*   **`@middleware.validate(ValidatorClass)`**: Triggers the validation machinery. If `ValidatorClass.validate()` returns errors, it terminates the pipeline and raises a `ValidationError`.
*   **`@middleware.thread_safe(entity_type, entity_id_attr)`**: Ensures sequential execution. It acquires a lock for the specified entity ID before entering the handler, preventing concurrent state corruption.
*   **`@middleware.inject_attrs(injectors)`**: Dynamically populates command attributes before execution. Useful for injecting the `current_user_id` from a global context into the command DTO.
*   **`@middleware.persist_events()`**: Intercepts the `CommandResponse`, enriches any attached events with correlation IDs, and persists them to the storage backend in a single batch.

#### Complex Pipeline Example
```python
from cqrs_ddd.middleware import middleware

@middleware.log()
@middleware.persist_events()
@middleware.thread_safe(entity_type="Account", entity_id_attr="account_id")
@middleware.validate(TransferMoneyValidator)
@middleware.inject_attrs({"operator_id": CurrentUserInjector()})
class TransferMoneyHandler(CommandHandler):
    async def handle(self, cmd: TransferMoney) -> CommandResponse:
        # Business logic is clean and focused
        ...
```

#### Visual Execution Order
For the example above, the flow of execution follows this sequence. Notice how it goes from the top-most decorator (Logging) deep into the handler, and then bubbles back up for post-processing:

```mermaid
sequenceDiagram
    participant M as Mediator
    participant L as Logging
    participant P as PersistEvents
    participant T as ThreadSafe
    participant V as Validate
    participant I as InjectAttrs
    participant H as Handler (Business Logic)

    M->>L: 1. Start Log
    L->>P: 2. Get/Gen Correlation ID
    P->>T: 3. Acquire Lock
    T->>V: 4. Validate Content
    V->>I: 5. Compute & Inject Attrs
    I->>H: 6. Execute handle()

    Note over H: Business Logic Happens

    H-->>I: 7. Return Result
    I-->>V: 8. Pass Result
    V-->>T: 9. Pass Result
    T-->>P: 10. Release Lock
    P-->>L: 11. Persist Events Batch
    L-->>M: 12. Finalize Log & Return
```
Validation is first-class. Handlers can be decorated with `@middleware.validate(MyValidator)` to enforce rules.

*   **`ValidationResult`**: A standardized DTO for error reporting.
*   **`PydanticValidator`**: Base class for rich validation, supporting both static schema checks and async business rules (e.g., unique email check).

```python
from cqrs_ddd.middleware import middleware
from cqrs_ddd.validation import PydanticValidator

class CreateUserValidator(PydanticValidator):
    async def validate_email(self, value):
        if await email_exists(value):
            raise ValueError("Email already registered")

@middleware.log()
@middleware.validate(CreateUserValidator)
class CreateUserHandler(CommandHandler):
    ...
```

### 3. Event Hydration & Routing
The toolkit provides a high-performance engine for reconstructing and dispatching events.

*   **`EventTypeRegistry`**: Maps event type names to Python classes.
    *   **Hydration**: Automatically reconstructs full Domain Event objects from JSON payloads stored in your database or queue.
*   **`EventDispatcher`**:
    *   **Priority Handlers (`is_priority=True`)**: Executed **synchronously** within the active Unit of Work. If a priority handler fails, the transaction rolls back, ensuring strong consistency (e.g., updating a critical read model).
    *   **Background Handlers**: Executed as fire-and-forget `asyncio` tasks after the transaction commits.
    *   **Concurrency Control**: Uses a configurable **Semaphore** to limit concurrent background executions, preventing system overload.

---

## Infrastructure Tier

The Infrastructure Tier provides a unified abstraction for all data persistence and retrieval, decoupling your Domain and Application layers from specific storage implementations.

### 1. Unified Persistence Dispatcher
The `PersistenceDispatcher` is the central brain of the infrastructure layer. It uses a **Double-Dispatch** mechanism to resolve the correct persistence logic based on both the **Entity Type** and the **Operation Type**.

#### Key Responsibilities:
*   **Transaction Management**: Orchestrates Units of Work (UOW) across multiple handlers.
*   **Dynamic Resolution**: Auto-discovers and executes handlers based on generic type arguments.
*   **Read-Through Cache Management**: Transparently manages high-performance caching for both Aggregates and Read Models.
*   **Secondary Effects**: Supports multiple handlers per operation (e.g., primary DB write + secondary audit log write).

#### Magical Auto-Registration
The toolkit eliminates manual registration boilerplate. By using **Python Generics**, your handlers are automatically discovered and registered with the dispatcher the moment the class is defined.

*   **Type Inference**: The system introspects the generic type argument (e.g., `OperationPersistence[UserCreated]`) to map the handler to the correct event/entity.
*   **Union Type Support**: A single handler can manage multiple types (e.g., `OperationPersistence[Union[UserCreated, UserUpdated]]`).

#### Priority & Execution Pipelines
The `PersistenceDispatcher` doesn't just find one handler; it resolves **all** handlers for a given type and executes them in a deterministic pipeline based on their `priority`.

*   **Primary vs Secondary**: Use high priority (e.g., `100`) for your primary database write and lower priority (e.g., `10`) for secondary effects like search indexing or audit logging.
*   **Final Result**: The dispatcher executes all handlers in the pipeline, but returns the result from the **highest priority** (first) handler to the caller.


---

### 2. Persistence Protocols

The toolkit defines three distinct protocols to separate Command-Side and Query-Side responsibilities.

#### A. Operation Persistence (Command Writes)
Handles the persistence of state changes. It is usually triggered by a `Modification` object from the Domain layer.

```python
from cqrs_ddd.persistence_dispatcher import OperationPersistence

# 1. Define the Handler
class SaveUserHandler(OperationPersistence[UserCreated]):
    priority = 100 # Ensure this runs FIRST and provides the primary result

    async def persist(self, modification: UserCreated, uow):
        # modification.entity contains the aggregate
        # uow contains the database session
        user_record = UserRecord.from_domain(modification.entity)
        await uow.session.merge(user_record)
        return modification.entity.id

# 2. Usage via Dispatcher
await dispatcher.apply(UserCreated(user))
```

#### B. Retrieval Persistence (Command-Side Reads)
Used to load full domain aggregates for processing commands. It is optimized for high-performance "read-before-write" patterns.

```python
from cqrs_ddd.persistence_dispatcher import RetrievalPersistence

class UserRetrievalHandler(RetrievalPersistence[User]):
    entity_name = "User" # Enables Read-Through Caching
    cache_ttl = 300

    async def retrieve(self, entity_ids: List[str], uow) -> List[User]:
        records = await uow.session.query(UserRecord).filter(id__in=entity_ids).all()
        return [r.to_domain() for r in records]

# Usage:
user = await dispatcher.fetch_domain_one(User, "uuid-123")
```

#### C. Query Persistence (Query-Side Reads)
Standalone persistence for Fetching optimized Read Models or DTOs. Completely decoupled from Domain Entities.

```python
from cqrs_ddd.persistence_dispatcher import QueryPersistence

class UserDashboardHandler(QueryPersistence[UserDashboardDTO]):
    entity_name = "UserDashboard"

    async def fetch(self, entity_ids: List[str], uow) -> List[UserDashboardDTO]:
        # Raw SQL or optimized query for visualization
        return [UserDashboardDTO(...)]

# Usage:
dto = await dispatcher.fetch_one(UserDashboardDTO, "uuid-123")
```

---

### 3. Advanced Features

#### Read-Through Caching & Auto-Invalidation
The `PersistenceDispatcher` provides a "transparent" caching layer. By setting `entity_name` on your handlers, the dispatcher automatically:
1.  **Checks Cache**: Before calling `retrieve()` or `fetch()`, it checks if the IDs are in Redis/Memory.
2.  **Fills Cache**: On a miss, it calls the handler and populates the cache with the results.
3.  **Invalidates on Write**: When `dispatcher.apply(mod)` is called for an entity, it **automatically deletes** all related cache entries (both Domain and Query models) to ensure consistency.

#### Multiple Handlers & Priority
You can register multiple handlers for the same `Modification`. They will execute in order of `priority` (highest first).

```python
class AuditLogPersistence(OperationPersistence[UserCreated]):
    priority = 10 # Runs AFTER SaveUserHandler (priority 100)

    async def persist(self, mod, uow):
        await uow.session.add(AuditEntry(action="USER_CREATED", id=mod.entity.id))
```

#### Caching Opt-out (High-Volume Entities)
For entities with **High Write Volume / Low Read Volume** (e.g., telemetry, audit logs, or massive datasets), you can completely disable the caching lifecycle (both lookups and invalidations) by setting `use_cache = False` on your handler:

```python
class TelemetryHandler(OperationPersistence[MetricRecorded]):
    use_cache = False  # Skip Redis invalidation on every write

    async def persist(self, modification, uow):
        # High-speed insert logic...
        ...
```

*   **Reads**: The dispatcher will bypass `_execute_read_through` and hit the database directly.
*   **Writes**: The dispatcher will skip calling `delete_batch` on the cache service, saving network roundtrips to Redis.

---

### 4. Event Sourcing & Time Travel

The toolkit implements a robust Event Sourcing foundation that provides a perfect audit trail and enables advanced features like optimistic concurrency and undo/redo.

#### A. StoredEvent
Events are persisted as `StoredEvent` objects, capturing the full intent and context:
- **Audit Metadata**: Correlation ID (links original command), Causation ID (links preceding event), and User ID.
- **Versioning**: Each event is assigned an `aggregate_version` for optimistic concurrency.
- **Payload**: The serialized event data, ready for hydration.

#### B. EventStore Middleware
The `EventStoreMiddleware` is a flagship example of the toolkit's pipeline system. It handles cross-cutting audit concerns automatically:
1.  **Context Enrichment**: Injects tracking IDs and user info into all events.
2.  **Atomic Persistence**: Saves events to the `EventStore` only if the handler succeeds.
3.  **Zero Boilerplate**: Apply to any Command Handler via the decorator.

```python
@middleware.apply(EventStoreMiddleware, event_store=my_store)
class CreateOrderHandler(CommandHandler):
    ...
```

#### C. Undo/Redo Engine
The system supports **Soft Undos**, ensuring that history is never modified, only appended to.

*   **Auto-Registration**: Executors use `Generic[T]` to automatically pair themselves with domain events. No manual registry wiring is required.
*   **Undo Service**: The `UndoService` manages the complex lifecycle of reversals, including stack retrieval (`get_undo_stack`) and state invalidation.
*   **Business Rules (`can_undo`)**: Before any reversal, the system calls `can_undo()` on the executor. This allows you to block undos based on domain state (e.g., "Cannot undo Payment if Order is already Shipped").
*   **Soft History**: Events are marked `is_undone=True` rather than deleted. Compensating events are appended to the store, pointing back to the original via `causation_id`.

```python
class PriceUpdateUndo(UndoExecutor[PriceUpdated]):
    # Optional logic to check if undo is still valid
    async def can_undo(self, event: PriceUpdated) -> bool:
        return not event.entity.is_locked

    async def undo(self, event: PriceUpdated):
        # Generate compensating event to restore the old price
        return [PriceUpdated(new_price=event.old_price)]

    async def redo(self, event: PriceUpdated, undo_event: PriceUpdated):
        # Restore the originally "undone" price change
        return [PriceUpdated(new_price=event.new_price)]
```

#### Asymmetric Reversals
In complex domains, an "Undo" is not always a simple mirror of the original action. The toolkit handles this by providing both the **Original Event** and the **Undo Event** to the `redo` method:

*   **Undo Create**: Produces a `Deleted` event.
*   **Redo Delete**: Might produce a `Restored` event rather than just repeating `Created`.

This dual-context approach ensures that redoing an action can account for any state changes that occurred during the "undone" period.

*   **UndoResult**: Provides a unified response with `success` status, lists of `undone_events`, and any `compensating_events` created.
*   **Redo Logic**: By tracking `causation_id`, the system can "undo the undo", perfectly restoring state while maintaining a full audit log.

---

### 5. Transactional Outbox
Guarantees **At-Least-Once Delivery** for domain events by solving the "Dual Write Problem". Instead of publishing to a broker (like RabbitMQ) during a database transaction—which could fail or leave state inconsistent—events are persisted into a dedicated `Outbox` table within the same ACID transaction.

#### Reliable Infrastructure
*   **Zero Data Loss**: Even if your broker is down or the network fails, events remain safe in your primary database.
*   **Atomic Persistence**: Uses the `OutboxPublisher` to intercept events and redirect them to storage automatically.
*   **Status Lifecycle**: Messages transition through `pending` ➔ `processing` ➔ `published` (or `failed`).

#### Outbox Worker
The `OutboxWorker` (or a distributed task like Dramatiq/Celery) polls the storage and ensures events reach their destination:

```python
from cqrs_ddd.outbox import OutboxWorker, OutboxService

# Setup the background processor
worker = OutboxWorker(
    storage=SQLAlchemyOutboxStorage(session),
    publisher=RabbitMQPublisher(connection),
    poll_interval=1.0,  # Hyper-responsive polling
    batch_size=100      # Efficient batching
)

# Start the non-blocking loop
await worker.start()
```

*   **Batching Strategy**: Processes events in configurable sizes to maximize throughput without overloading the DB.
*   **Automatic Retries**: Failed messages are marked with the error stack and retried with configurable limits.
*   **Traceability**: Each outbox message maintains its original `correlation_id`, ensuring a complete audit trail across services.

---


## Storage Services
The toolkit abstracts file storage behind a unified protocol, allowing you to switch between Local, S3, Dropbox, and Google Cloud Storage without changing your application logic.

### Supported Backends
Each backend implements the `StorageService` protocol (`save`, `read`, `delete`, `exists`, `list`).

#### 1. Local Filesystem (Default)
Safe, atomic file operations using `aiofiles`. Perfect for development or single-node deployments.

```python
from cqrs_ddd.backends.storage import LocalFileSystemStorage

storage = LocalFileSystemStorage(root_path="./uploads")
await storage.save("report.pdf", b"...")
```

#### 2. AWS S3
Uses `aiobotocore` for high-performance async S3 access.

```python
from cqrs_ddd.backends.storage import S3Storage

storage = S3Storage(
    bucket="my-app-assets",
    aws_access_key_id="...",
    aws_secret_access_key="...",
    region_name="us-east-1"
)
```

#### 3. Dropbox
Uses `aiohttp` to interact with the Dropbox API v2.

```python
from cqrs_ddd.backends.storage import DropboxStorage

# Access token with 'files.content.write' scope
storage = DropboxStorage(
    access_token="...",
    root_path="/app-backups"  # Optional: Confine to this folder (acts like a bucket)
)
```

#### 4. Google Cloud Storage
Uses `gcloud-aio-storage`.

```python
from cqrs_ddd.backends.storage import GoogleCloudStorage

storage = GoogleCloudStorage(
    bucket="my-gcs-bucket",
    service_file="path/to/creds.json"
)
```

---

## Infrastructure Backends
Detailed guide to the core infrastructure implementations included in the toolkit.

### SQLAlchemy Backend
The `cqrs_ddd.backends.sqlalchemy` module provides a robust, async-first persistence layer using `sqlalchemy.ext.asyncio`.

#### 1. Unit of Work
The `SQLAlchemyUnitOfWork` manages database transactions. It ensures that all changes (Entity saves, Event appends, Outbox messages) are committed atomically.

*   **Atomic Commit**: `uow.commit()` finalizes the transaction.
*   **Automatic Rollback**: If an exception occurs in the context manager, `uow.rollback()` is triggered.

#### 2. Event Store
`SQLAlchemyEventStore` persists domain events to a PostgreSQL table (default: `domain_events`) using `JSONB`.

*   **Schema**:
    *   `id`: UUID
    *   `aggregate_id`: UUID/String (Indexed)
    *   `aggregate_version`: Integer (Optimistic Locking)
    *   `payload`: JSONB
    *   `correlation_id`: String (Indexed)
*   **Batching**: `append_batch()` writes multiple events in a single `INSERT` statement for performance.

#### 3. Transactional Outbox
`SQLAlchemyOutboxStorage` implements the storage side of the Outbox Pattern.

*   **Polling Optimization**: Uses `FOR UPDATE SKIP LOCKED` to allow multiple workers to poll the table concurrently without contending for the same messages.
*   **Status Tracking**: Messages track `retries` and `last_error` to support exponential backoff.

#### 4. Saga Repository
`SQLAlchemySagaRepository` persists long-running process state (`SagaContext`).

*   **State Serialization**: Stores arbitrary saga state as `JSONB`, allowing for schema evolution without migration headaches.
*   **History Tracking**: Keeps a log of processed steps within the JSON payload.

### RabbitMQ Backend
The `RabbitMQBroker` (in `cqrs_ddd.backends.rabbitmq`) provides industry-standard messaging using `aio_pika`.

*   **Exchange**: Publishes to a `TOPIC` exchange (default: `domain_events`).
*   **Queues**: Consumers create durable queues bound to specific routing keys (e.g., `order.created`).
*   **Reliability**: Uses `publisher_confirms` ensures messages reach the broker.

### Redis Services
The Redis backend provides high-performance caching and distributed coordination.

#### 1. Distributed Lock (`RedisLockStrategy`)
Implements a **Fair Locking** algorithm to ensure FCFS (First-Come, First-Served) access to Aggregates and Sagas.

*   **Fairness**: Uses a Redis `ZSET` (timestamp-scored set) to queue waiters. You only get the lock if you are at the head of the line.
*   **TTL**: All locks have a timeout to prevent deadlocks if a worker crashes.

#### 2. Cache Service (`RedisCacheService`)
Implements `CacheService` for read-through caching of Entities and Views.

*   **Compression**: Uses `zlib` to compress payloads, saving RAM.
*   **Serialization**: Uses `pickle` by default for full Python object fidelity (can be overridden).
*   **Batching**: Supports `get_batch` / `set_batch` (MGET/MSET/Pipeline) for bulk operations.

---

## Error Handling & Exceptions

The toolkit provides a standard hierarchy of exceptions, which the web framework integrations automatically map to appropriate HTTP status codes.

| Exception Class | Description | HTTP Status |
| :--- | :--- | :--- |
| **`DomainError`** | Business logic violation (e.g., "Product already shipped"). | `400 Bad Request` |
| **`ValidationError`** | Input schema or constraint violation. | `422 Unprocessable Entity` |
| **`EntityNotFoundError`** | Requested resource/aggregate ID does not exist. | `404 Not Found` |
| **`ConcurrencyError`** | Optimistic lock collision (version mismatch). | `409 Conflict` |
| **`AuthorizationError`** | Permission denied for the requested operation. | `403 Forbidden` |
| **`InfrastructureError`** | External system failure (DB, Broker, etc.). | `503 Service Unavailable` |

### Custom Handling
You can catch these exceptions in your application logic to implement custom recovery strategies or user-friendly error messages:

```python
from cqrs_ddd.exceptions import DomainError

try:
    await mediator.send(ShipOrder(order_id="123"))
except DomainError as e:
    logger.warning(f"Shipping failed: {e.message}")
```

---

## Process Management (Sagas)
Sagas coordinate long-running distributed transactions through **Orchestration** or **Choreography**. They maintain state across multiple independent transactions, ensuring that eventual consistency is achieved even when individual steps fail.

```mermaid
sequenceDiagram
    participant M as Mediator
    participant SM as Saga Manager
    participant R as Saga Repository
    participant L as Lock Strategy
    participant S as Saga Instance

    M->>SM: Handle Event
    SM->>L: Acquire Lock (Correlation ID)
    SM->>R: Load Context
    SM->>S: Apply Step (Event)
    activate S
    S-->>S: Update Internal State
    S-->>S: Queue Pending Commands
    deactivate S
    SM->>R: Save Context
    SM->>M: Dispatch Pending Commands
    SM->>L: Release Lock
```

### Management Patterns

The toolkit supports two primary ways to manage saga lifecycles:

#### 1. Choreography (`SagaChoreographyManager`)
Sagas react to domain events published by aggregates. This is the most decoupled approach.

```mermaid
graph LR
    A[Aggregate] -- "Produces" --> E1[Event A]
    E1 -- "Triggers" --> CM[Choreography Manager]
    CM -- "Loads/Steps" --> S[Saga Instance]
    S -- "Dispatches" --> C1[Command B]
    C1 -- "Mutates" --> B[Another Aggregate]
    B -- "Produces" --> E2[Event B]
    E2 -- "Triggers" --> CM
```

#### 2. Orchestration (`SagaOrchestratorManager`)
A central manager explicitly drives the saga. This is useful when the saga is the primary entry point for a complex business process started by a command.

```mermaid
graph TD
    CH[Command Handler] -- "Starts" --> OM[Orchestrator Manager]
    OM -- "Runs" --> S[Saga Instance]
    S -- "Dispatches" --> C1[Command 1]
    C1 -- "Processed by" --> H1[Handler 1]
    H1 -- "Returns Result" --> OM
    OM -- "Continues" --> S
    S -- "Dispatches" --> C2[Command 2]
```

### Core Components

*   **`SagaContext`**: The persistent state of a saga instance. It tracks:
    - `state`: Custom business data (dict or Pydantic model).
    - `history`: An audit trail of every event handled and command dispatched.
    - `processed_message_ids`: Ensures **Idempotency** by never processing the same event twice.
    - `parent_saga_id`: Support for sub-sagas.
*   **`@saga_step`**: A declarative decorator that maps Domain Events to specific handler methods.
*   **`SagaManager`**: The base orchestrator. Subclasses include:
    - **`SagaChoreographyManager`**: Reacts to domain events (`handle_event`).
    - **`SagaOrchestratorManager`**: Explicitly starts sagas via `run()`.
*   **`SagaRepository`**: Persists saga state (InMemory, SQLAlchemy). Protocol now enforces `saga_type` in lookups for precision.
*   **`SagaRegistry`**: Explicitly maps Domain Events to Saga classes. Must be passed to the Manager.
*   **`PydanticSaga`**: Type-safe saga state management using Pydantic models.

### Explicit Registration (Strict DI)
The toolkit enforces explicit Dependency Injection. You must register your Sagas manually to avoid global state side effects.

```python
from cqrs_ddd.saga_registry import SagaRegistry

# 1. Configure Registry
registry = SagaRegistry()
registry.register(OrderCreated, OrderSaga)
registry.register(PaymentProcessed, OrderSaga)

# 2. Inject into Manager
manager = SagaChoreographyManager(repo, mediator, saga_registry=registry)
```

### Failure Handling & Compensation
Unlike traditional ACID transactions, Sagas use **Compensating Transactions** to undo failures.
- **Automatic Compensation**: If a step raises an exception, `compensate()` is triggered.
- **Stalled Recovery**: The `recover_pending_sagas` method (and `find_stalled_sagas` repository method) allows resuming sagas that crashed mid-dispatch.

#### Example: Type-Safe Order Saga (Pydantic)

```python
from cqrs_ddd.contrib.pydantic import PydanticSaga, PydanticDomainEvent
from cqrs_ddd.saga import saga_step
from pydantic import BaseModel

class OrderState(BaseModel):
    order_id: str = ""
    status: str = "pending"

class OrderSaga(PydanticSaga[OrderState]):
    state_model = OrderState

    @saga_step(OrderCreated)
    async def on_order_created(self, event: OrderCreated):
        """Step 1: Init state and trigger payment."""
        self.state.order_id = event.order_id
        self.dispatch_command(ProcessPayment(amount=event.total))

    @saga_step(PaymentProcessed)
    async def on_payment(self, event: PaymentProcessed):
        """Step 2: Complete or Comp."""
        if event.success:
            self.state.status = "paid"
            self.dispatch_command(ShipOrder(order_id=self.state.order_id))
            self.complete()
        else:
            raise ValueError("Payment Failed") # Triggers compensate()

    async def compensate(self):
        """Rollback: Cancel order."""
        self.dispatch_command(CancelOrder(order_id=self.state.order_id))

```

### Human-in-the-Loop (HitL) & Timeouts
Sagas can pause execution ("suspend") to wait for external signals, such as manual user approval or 3rd party webhooks.

*   `suspend(reason: str, timeout: timedelta)`: Marks saga as suspended. It will not be flagged as "stalled".
*   `resume()`: Clears the suspension. Must be called explicitly in the handler that receives the unblocking event.
*   `on_timeout()`: Hook called if the saga remains suspended past its timeout. Defaults to failing the saga.

```python
    @saga_step(LargeOrderPlaced)
    async def on_large_order(self, event):
        # Wait up to 24 hours for a manager to approve
        self.suspend("Waiting for Manager Approval", timeout=timedelta(hours=24))

    @saga_step(OrderApproved)
    async def on_approval(self, event):
        self.resume() # Clear suspension
        self.dispatch_command(ShipOrder(...))
```

#### Timeout Processing
To handle expired suspensions, run the maintenance task periodically:
```python
# Finds expired sagas, loads them, and calls on_timeout()
await saga_manager.process_timeouts()
```

### Orchestration Lifecycle
1.  **Start**: `SagaOrchestratorManager.run(SagaClass, initial_data)` or `SagaChoreographyManager.handle_event(event)`.
2.  **Concurrency**: Uses a `LockStrategy` (Redis/SQL) to serialize access to the same saga instance.
3.  **Step Execution**: State is active. Modifying `self.context.state` (or `self.state` in Pydantic) updates the dirty state.
4.  **Persistence**: `SagaRepository.save()` persists the context.
5.  **Dispatch**: Commands in `pending_commands` are dispatched via `Mediator`. If dispatch fails, they remain pending for recovery.

### Repository & Protocol
The `SagaRepository` protocol ensures strict typing:
```python
# Unambiguous Lookup
context = await repo.find_by_correlation_id(correlation_id="123", saga_type="OrderSaga")

# Crash Recovery
stalled = await repo.find_stalled_sagas()
```

---

## Messaging Architecture
The toolkit provides a "Protocol-First" messaging infrastructure to bridge the gap between internal domain events and external systems (RabbitMQ, Kafka, Dramatiq).

### Outbound: Publishers & Routing
To publish events, the toolkit uses the **Interception Pattern**. You don't call the broker directly; instead, the `EventDispatcher` routes events to a `PublishingEventHandler`.

#### 1. Selective Routing with `@route_to`
Use the `@route_to` decorator to tag events for specific destinations. This decouples your domain logic from your infrastructure routing.

```python
from cqrs_ddd.publishers import route_to

@route_to("dramatiq")
class OrderPlaced(DomainEvent): ...

@route_to("rabbitmq")
class PaymentSuccess(DomainEvent): ...
```

#### 2. `TopicRoutingPublisher`
A sophisticated router that decides where to send a message based on a hierarchy:
1.  **Event Key**: Looks for a destination tagged via `@route_to`.
2.  **Explicit Route**: Check a manual topic-to-publisher mapping.
3.  **Default**: Fallback to a catch-all publisher (e.g., the Outbox).

### Inbound: BaseEventConsumer
The `BaseEventConsumer` acts as a bridge for incoming messages. It implements the "Inbox pattern" with three automated steps:

1.  **Subscribe**: Listens to one or more topics on the broker.
2.  **Hydrate**: Uses the `EventTypeRegistry` to transform JSON payloads back into rich Python objects.
3.  **Dispatch**: Sends the hydrated event to the local `EventDispatcher` for background processing.

```python
from cqrs_ddd.consumer import BaseEventConsumer

consumer = BaseEventConsumer(
    broker=RabbitMQConsumer(),
    dispatcher=local_dispatcher,
    topics=["OrderCreated", "PaymentValidated"],
    queue_name="fulfillment-service"
)

await consumer.start()
```

---

## Integrations

### Pydantic V2
The toolkit is fully optimized for Pydantic V2, providing a seamless bridge between your API layer and your domain logic.

#### Command & Query Models
Inherit from `PydanticCommand` or `PydanticQuery` to gain automatic validation and serialization. These classes manage internal metadata (like `command_id` and `correlation_id`) using Pydantic `PrivateAttr`.

```python
from cqrs_ddd.contrib.pydantic import PydanticCommand

class ChangeAddress(PydanticCommand):
    street: str = Field(..., min_length=5)
    postcode: str = Field(..., pattern=r"^[A-Z0-9]{5}$")
```

#### Domain Building Blocks
*   **`PydanticValueObject`**: Immutable models with value-based equality. Uses `frozen=True` to prevent state mutation.
*   **`PydanticEntity`**: Includes built-in versioning, audit trails (`created_at`, `updated_at`), and soft delete logic. Supports `increment_version()`, `soft_delete()`, and `restore()`.
*   **`PydanticDomainEvent`**: Fully serializable events that handle their own hydration and auto-registration lifecycle.

#### Typed State Sagas
Use `PydanticSaga[T]` to manage complex process state with full type safety. The toolkit automatically synchronizes your Pydantic model back to the persistent `SagaContext` at the end of every step.

```python
class MySagaState(BaseModel):
    step_count: int = 0

class MyProcessSaga(PydanticSaga[MySagaState]):
    state_model = MySagaState

    @saga_step(SomeEvent)
    async def on_event(self, event):
        self.state.step_count += 1
        # Success: self.state is synced back to context.state automatically
```

#### Async Validation Lifecycle
The `PydanticValidator` provides a sophisticated async validation pipeline:
1.  **`init_context`**: Use this for async prep (e.g., checking if an email exists in the DB).
2.  **`validate_*`**: Implement field-specific validation methods that run in sequence.

```python
class CreateUserValidator(PydanticValidator[CreateUser]):
    async def init_context(self, cmd: CreateUser):
        self.exists = await self.user_repo.exists_by_email(cmd.email)

    def validate_email(self, email: str):
        if self.exists:
            raise ValueError("Email already taken")
```

#### Infrastructure & Response Models
*   **`PydanticOutboxMessage`**: A Pydantic-native version of outbox messages for API consumption.
*   **`CommandResult` & `QueryResult`**: Standardized response wrappers for consistent JSON output.

#### Technical Configuration
Every base class comes pre-configured for Domain-Driven accuracy:
- **`arbitrary_types_allowed=True`**: Allows using custom types (like `EntityID`) within models.
- **`frozen=True`**: Enforced on Value Objects and Events to guarantee immutability.
- **`validate_assignment=True`**: Enforced on Entities to ensure valid state transitions.

> [!NOTE]
> Pydantic is an optional dependency. Install it using: `pip install py-cqrs-ddd-toolkit[pydantic]`

---

### Dependency Injector
The toolkit includes official support for [python-dependency-injector](https://python-dependency-injector.ets-labs.org/) to handle complex IoC (Inversion of Control) requirements.

#### Application Container & Composition
For production apps, it is recommended to wrap the toolkit's `CoreContainer` inside your application's own container. This enables you to manage toolkit settings alongside your business dependencies.

```python
from dependency_injector import containers, providers
from cqrs_ddd.contrib.dependency_injector import Container as CoreContainer

class AppContainer(containers.DeclarativeContainer):
    # Nest the toolkit core
    core = providers.Container(CoreContainer)

    # Define your business services
    user_repo = providers.Singleton(SqlAlchemyUserRepo)
```

#### Initialization & Required Overrides
The `CoreContainer` defines several **Abstract Dependencies** (using `providers.Dependency()`) that you must override during your application's bootstrap phase (e.g., in a FastAPI lifespan or script entry point):

1.  **`uow_factory`**: (Required) Function that returns a Unit of Work (e.g., `SQLAlchemyUoW`).
2.  **`message_broker`**: (Required) The publisher implementation for the Outbox (e.g., `RabbitMQPublisher`).

```python
from cqrs_ddd.contrib.dependency_injector import install as install_di

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Initialize your container
    container = AppContainer()
    container.core.config.from_dict({
        "scan_packages": ["myapp.application"]
    })

    # 2. Provide required infrastructure overrides
    container.core.uow_factory.override(
        create_uow_factory(session_factory=my_session_maker)
    )
    container.core.message_broker.override(
        RabbitMQPublisher(url="...")
    )

    # 3. Complete the wiring
    container.wire(packages=["myapp.api", "myapp.application"])
    container.core.wire(packages=["myapp.api", "myapp.application"])

    # 4. Install wired middlewares (Enables @inject in @middleware stacks)
    install_di()

    yield
```

#### Automated Event Routing
The `autowire_router` resource automatically scans your domain packages for events tagged with `@route_to` and registers them to the correct publisher/broker.

Simply provide the `scan_packages` configuration to the core container:

```python
# Pass the list of packages to scan (strings)
container.core.config.from_dict({
    "scan_packages": ["myapp.domain", "myapp.application"]
})
```

---

### Dramatiq (Task Queue)
The toolkit integrates with [Dramatiq](https://dramatiq.io/) for robust background task processing and distributed event handling.

#### 1. Setup & Configuration
Configure the broker with RabbitMQ and Redis (for results/aborting) using the helper:

```python
from cqrs_ddd.contrib.dramatiq_tasks import setup_dramatiq

# Configures global dramatiq.set_broker()
broker = setup_dramatiq(
    broker_url="amqp://guest:guest@localhost:5672",
    redis_url="redis://localhost:6379/0",
    enable_asyncio=True
)
```

#### 2. Event Publishing & Routing
The `DramatiqEventPublisher` publishes domain events by sending them to a routing actor (`default_domain_event_router`).

**Note**: By default, this actor listens on the **"cqrs_ddd_dramatiq_events"** queue. The `topic` argument is used for logical dispatching within the worker, not for queue selection.

**Advanced: Custom Queue Control**
To use a custom queue name (e.g., `cqrs_events`), define your actor and override the default routing logic.

**1. Define Custom Actor**
```python
@dramatiq.actor(queue_name="cqrs_events")
async def custom_router(msg):
    # Determine dispatcher (manual or from global)
    # Re-use library helper:
    await handle_dramatiq_event(msg, get_dispatcher())
```

**2a. With Container (Recommended)**
Override the default actor provider in your container subclass. The toolkit's wiring ensures `dramatiq_event_publisher` will pick this up automatically.

```python
from dependency_injector import providers
from cqrs_ddd.contrib import dependency_injector

class MyContainer(dependency_injector.Container):
    # Override the routing actor to use your custom one
    dramatiq_routing_actor = providers.Object(custom_router)

    # success! 'dramatiq_event_publisher' now targets 'custom_router'
```

**2b. Without Container (Manual)**
Manually inject the actor into the publisher:
```python
publisher = DramatiqEventPublisher(routing_actor=custom_router)
await publisher.publish("OrderPlaced", order_evt)
```

**Crucial**: The routing actor runs *in your worker process* and needs access to your application's `EventDispatcher` to handle events. You must wire this connection at startup.

```python
from cqrs_ddd.contrib.dramatiq_tasks import set_dispatcher_resolver

# Must be called in your 'worker.py' or main entry point
set_dispatcher_resolver(lambda: container.core.event_dispatcher())
```

#### 3. Worker Entry Point (`worker.py`)
If running workers via the CLI or script, ensure you initialize the container and resolver:

```python
import dramatiq
from cqrs_ddd.contrib.dramatiq_tasks import setup_dramatiq, set_dispatcher_resolver
from myapp.container import AppContainer

# 1. Setup Broker
broker = setup_dramatiq(broker_url="...")

# 2. Init Container & Wire Resolver
container = AppContainer()
set_dispatcher_resolver(lambda: container.core.event_dispatcher())

# 3. Import actors (so Dramatiq finds 'default_domain_event_router')
import cqrs_ddd.contrib.dramatiq_tasks
```

#### 4. Embedded Worker (Dev/Simple)
For simple deployments, you can run a Dramatiq worker embedded in your application:

```python
from cqrs_ddd.contrib.dramatiq_tasks import DramatiqWorkerConsumer

consumer = DramatiqWorkerConsumer(broker, queues=["default"])
await consumer.start()
```

---

## Observability (Tracing)
The toolkit provides built-in support for distributed tracing, allowing you to visualize complex command-side flows and cross-service saga executions.

### Configuration
Support for **OpenTelemetry** and **Sentry** comes out of the box.

```python
from cqrs_ddd.contrib.tracing import configure_tracing

# Backends: "opentelemetry", "sentry", or "noop"
configure_tracing(backend_name="opentelemetry", service_name="order_service")
```

### Instrumentation
Automatically trace your core components without modifying your handlers:

```python
from cqrs_ddd.contrib.tracing import instrument_dispatcher, instrument_saga_manager, traced_saga, traced_persistence

# 1. Trace the Persistence Layer
instrument_dispatcher(persistence_dispatcher)

# 2. Trace Distributed Sagas
instrument_saga_manager(saga_manager)

# 3. Trace specific Saga classes
@traced_saga
class ShipmentSaga(Saga): ...

# 4. Trace Custom Persistence Backends
@traced_persistence
class MyCustomPersistence(OperationPersistence): ...
```

### Deep Dive: Span Attributes
The toolkit automatically enriches spans with semantic attributes to help you query traces:
*   **`messaging.operation`**: The type of message (`command`, `query`, `event`, `orchestration`).
*   **`messaging.destination`**: The class name of the message being processed.
*   **`messaging.correlation_id`**: The causal ID connecting the entire flow (vital for cross-service tracing).
*   **`messaging.message_id`**: The unique ID of the specific message.

### Tracing Middleware
To trace the execution of Commands and Queries within the Mediator pipeline (including time spent in other middlewares), apply the `TracingMiddleware` to your handlers:

```python
from cqrs_ddd.middleware import middleware
from cqrs_ddd.contrib.tracing import TracingMiddleware

@middleware.apply(TracingMiddleware)
class CreateOrderHandler(CommandHandler):
    ...
```

---

## Web Framework Integration
The toolkit is designed to be framework-agnostic but includes first-class support for **FastAPI** and **Django**.

### FastAPI
Integration uses a declarative `CQRSRouter` to map Commands and Queries to endpoints.

#### 1. Setup (`main.py`)
Initialize the toolkit within your lifespan handler:

```python
from fastapi import FastAPI
from cqrs_ddd.contrib.fastapi import init_cqrs

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Initialize DB & Container
    container = MyContainer()
    container.wire(modules=[...])

    # 2. Init resources (auto-wiring events)
    container.init_resources()

    yield

    # Clean up
    await container.shutdown_resources()

app = FastAPI(lifespan=lifespan)

# 3. Register Global Exception Handlers & Hooks
init_cqrs(app, container.core)
```

#### 2. Declarative Routing
Reference: `demo/main.py`. Use `CQRSRouter` to define routes without writing boilerplate handlers:

```python
from cqrs_ddd.contrib.fastapi import CQRSRouter

router = CQRSRouter(container=container, prefix="/api/v1")

# Command (POST): Maps body -> Command -> Dispatch
router.command(
    "/products",
    CreateProduct,
    response_model=CommandResult,
    response_mapper=lambda r: {"id": r.result}
)

# Query (GET): Maps query params -> Query -> Dispatch
router.query(
    "/products/{id}",
    GetProduct,
    response_model=ProductDto
)

app.include_router(router.router)
```

#### 3. Background Workers
Hook the Outbox Worker into the startup lifecycle:

```python
from cqrs_ddd.contrib.fastapi import register_outbox_worker

worker = container.outbox_worker()
register_outbox_worker(app, worker)
```

---

### Django
Integration uses Class-Based Views (CBVs) and Middleware.

#### 1. Setup
Add the generic exception middleware to `settings.py`:

```python
MIDDLEWARE = [
    ...,
    "cqrs_ddd.contrib.django.CQRSExceptionMiddleware",
]
```

#### 2. Views (`CQRSView`)
Inherit from `CQRSView` to get automatic access to the `mediator`:

```python
from cqrs_ddd.contrib.django import CQRSView
from .commands import CreateProduct

class ProductView(CQRSView):
    container = app_container # Wire your container instance

    async def post(self, request):
        # 1. Parse body into Command
        cmd = self.parse_body(request, CreateProduct)

        # 2. Dispatch
        result = await self.dispatch_command(cmd)

        # 3. Return JSON
        return self.success({"id": result.result}, status=201)

    async def get(self, request, product_id):
        query = GetProduct(product_id=product_id)
        result = await self.dispatch_query(query)
        return self.success(result)
```

#### 3. Management Command
Run the outbox worker as a management command:

```python
# management/commands/run_outbox.py
from django.core.management.base import BaseCommand
from cqrs_ddd.contrib.django import run_outbox_worker

class Command(BaseCommand):
    def handle(self, *args, **options):
        run_outbox_worker(container.outbox_worker())
```

---

## Testing & Verification
The toolkit is built with testability in mind. Use the `InMemoryEventStore` and `InMemoryOutboxStorage` for fast unit tests without infrastructure overhead.

```bash
pytest --cov=src/cqrs_ddd tests/
```
