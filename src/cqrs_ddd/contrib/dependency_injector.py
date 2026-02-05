"""
Dependency Injector integration for CQRS/DDD toolkit.

This module provides:
1. A standard IoC Container for toolkit services.
2. Wired versions of standard middleware that automatically inject dependencies.
3. An `install()` function to register these wired middlewares.
"""

from typing import Optional, Any
from dependency_injector import containers, providers
from dependency_injector.wiring import inject, Provide

from ..persistence_dispatcher import PersistenceDispatcher
from ..dispatcher import EventDispatcher
from ..protocols import EventStore
from ..undo import DefaultUndoService, DefaultUndoExecutorRegistry
from ..event_store import InMemoryEventStore
from ..saga_registry import saga_registry as global_saga_registry
from ..middleware import (
    ThreadSafetyMiddleware,
    EventStorePersistenceMiddleware,
    middleware as middleware_registry,
)

try:
    from . import dramatiq_tasks

    HAS_DRAMATIQ = dramatiq_tasks.HAS_DRAMATIQ
    if HAS_DRAMATIQ:
        from .dramatiq_tasks import DramatiqWorkerConsumer
except ImportError:
    HAS_DRAMATIQ = False


# Validates and registers routed events at startup
def autowire_router_startup(
    dispatcher: EventDispatcher, publisher: Any, packages: Optional[list] = None
):
    if packages:
        from ..scanning import scan_packages

        scan_packages(packages)

    from ..publishers import auto_register_routed_events

    auto_register_routed_events(dispatcher, publisher)
    yield


class Container(containers.DeclarativeContainer):
    """
    IoC Container for the application.

    Provides specialized factories and singletons for core services.
    """

    config = providers.Configuration()

    # === 1. External Dependencies (Must be provided by app) ===

    # Message Broker (RabbitMQ, DramatiqPublisher, etc.)
    message_broker = providers.Dependency()

    # Dramatiq Providers (if installed)
    if HAS_DRAMATIQ:
        # Default routing actor (if not overridden by user)
        # User must call set_dispatcher_resolver(lambda: container.event_dispatcher()) at startup!
        dramatiq_routing_actor = providers.Object(
            dramatiq_tasks.default_domain_event_router
        )

        dramatiq_event_publisher = providers.Factory(
            "cqrs_ddd.contrib.dramatiq_tasks.DramatiqEventPublisher",
            routing_actor=dramatiq_routing_actor,
        )

        dramatiq_worker_consumer = providers.Factory(
            DramatiqWorkerConsumer,
            broker=message_broker,
            queues=config.dramatiq.queues,
            worker_threads=config.dramatiq.threads.as_int(),
        )

    # UoW Factory
    uow_factory = providers.Dependency()

    # Cache & Lock (Optional)
    cache_service = providers.Object(None)
    lock_strategy = providers.Object(None)

    # === 2. Core Infrastructure ===

    # Event Store
    event_store = providers.Singleton(InMemoryEventStore)

    # Outbox Storage
    outbox_storage = providers.Singleton(
        "cqrs_ddd.backends.sqlalchemy_outbox.SQLAlchemyOutboxStorage"
    )

    # Topic Router (Can route to different publishers based on event topic or destination key)
    # Configured via config.message_topic_router.routes (topic->pub) OR destinations (key->pub)
    # NOTE: Explicit routes (topic->pub) override @route_to auto-registration logic.
    _destinations = {"rabbit": message_broker}
    if HAS_DRAMATIQ:
        _destinations["dramatiq"] = dramatiq_event_publisher

    message_topic_router = providers.Factory(
        "cqrs_ddd.publishers.TopicRoutingPublisher",
        routes=config.message_topic_router.routes,
        destinations=providers.Dict(_destinations),
        default=message_broker,
    )

    # Outbox Service (The Gateway to the Broker)
    # This writes to DB Outbox AND has access to the real broker to push messages asynchronously
    outbox_publisher = providers.Factory(
        "cqrs_ddd.outbox.OutboxPublisher", storage=outbox_storage
    )

    outbox_service = providers.Singleton(
        "cqrs_ddd.outbox.OutboxService",
        storage=outbox_storage,
        publisher=message_topic_router,
    )

    # === 3. Event Handling ===

    # Event Dispatcher (Local + Outbox)
    # Reverted: EventDispatcher is now dumb. Use 'publishing_event_handler' to explicitly publish events.
    event_dispatcher = providers.Singleton(EventDispatcher)

    # Persistence Dispatcher
    persistence_dispatcher = providers.Singleton(
        PersistenceDispatcher,
        uow_factory=uow_factory,
        cache_service=cache_service,
    )

    # === 4. Application Services ===

    # Undo Service
    undo_registry = providers.Singleton(DefaultUndoExecutorRegistry)
    undo_service = providers.Singleton(
        DefaultUndoService,
        event_store=event_store,
        executor_registry=undo_registry,
        cache_service=cache_service,
    )

    # Mediator
    mediator = providers.Singleton(
        "cqrs_ddd.mediator.Mediator",
        uow_factory=uow_factory,
        event_dispatcher=event_dispatcher,
        container=providers.Object(None),
    )

    # Sagas
    saga_registry = providers.Object(global_saga_registry)
    saga_repository = providers.Singleton("cqrs_ddd.saga.InMemorySagaRepository")

    # Choreography Manager (Legacy SagaManager behavior)
    svc_saga_choreography_manager = providers.Singleton(
        "cqrs_ddd.saga.SagaChoreographyManager",
        repository=saga_repository,
        mediator=mediator,
        saga_registry=saga_registry,
        lock_strategy=lock_strategy,
    )

    # Orchestrator Manager (Explicit Orchestration)
    svc_saga_orchestrator_manager = providers.Singleton(
        "cqrs_ddd.saga.SagaOrchestratorManager",
        repository=saga_repository,
        mediator=mediator,
        saga_registry=saga_registry,
        lock_strategy=lock_strategy,
    )

    # Legacy alias or default implementation
    saga_manager = svc_saga_choreography_manager

    # Outbox Worker (Runs in background)
    outbox_worker = providers.Singleton(
        "cqrs_ddd.outbox.OutboxWorker", service=outbox_service
    )

    # Consumers
    event_consumer = providers.Factory(
        "cqrs_ddd.consumer.BaseEventConsumer",
        broker=message_broker,
        dispatcher=event_dispatcher,
        topics=config.consumer.topics,
        queue_name=config.consumer.queue_name,
    )

    # === Handlers ===

    # Generic handler to publish events to Outbox/Broker
    # Register this for any event you want to be distributed!
    publishing_event_handler = providers.Factory(
        "cqrs_ddd.publishers.PublishingEventHandler",
        publisher=outbox_publisher,  # Defaults to Outbox for safety
    )

    # Auto-wiring Resource
    # Initialize this resource to automatically scan packages and register @route_to events
    autowire_router = providers.Resource(
        autowire_router_startup,
        dispatcher=event_dispatcher,
        publisher=publishing_event_handler,
        packages=config.scan_packages,
    )


# =============================================================================
# Wired Middleware
# =============================================================================


class WiredThreadSafetyMiddleware(ThreadSafetyMiddleware):
    """ThreadSafetyMiddleware with auto-injection of lock_strategy."""

    @inject
    def __init__(
        self,
        entity_type: str,
        entity_id_attr: str,
        lock_strategy: Any = Provide[Container.lock_strategy],
    ):
        super().__init__(
            entity_type=entity_type,
            entity_id_attr=entity_id_attr,
            lock_strategy=lock_strategy,
        )


class WiredEventStorePersistenceMiddleware(EventStorePersistenceMiddleware):
    """EventStorePersistenceMiddleware with auto-injection of event_store."""

    @inject
    def __init__(self, event_store: EventStore = Provide[Container.event_store]):
        super().__init__(event_store=event_store)


def install():
    """
    Install dependency-injector integration.

    This overrides the default middleware classes in the registry
    with versions that support @inject.
    """
    middleware_registry.classes["thread_safety"] = WiredThreadSafetyMiddleware
    middleware_registry.classes["event_store"] = WiredEventStorePersistenceMiddleware
