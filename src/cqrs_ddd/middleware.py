from dataclasses import dataclass, field
from typing import Type, Callable, Any, Dict, Optional

from .protocols import Middleware, EventStore, LockStrategy
import logging
from datetime import datetime


@dataclass
class MiddlewareDefinition:
    """Definition of a middleware to be applied at runtime."""

    middleware_class: Type["Middleware"]
    kwargs: Dict[str, Any] = field(default_factory=dict)


class ValidatorMiddleware(Middleware):
    """
    Middleware for validation using a validation protocol.

    Supports both:
    - New Protocol: `validator.validate(message) -> ValidationResult`
    - Legacy: `Validator(message_dict).validate() -> ValidationResult`

    If validation fails, raises `ValidationError`.
    """

    def __init__(self, validator_class: Type = None):
        self.validator_class = validator_class

    def apply(self, handler_func: Callable, message) -> Callable:
        async def wrapped(*args, **kwargs):
            if self.validator_class:
                # Instantiate validator (assuming no-arg constructor or DI handling elsewhere)
                try:
                    validator = self.validator_class()
                    # New Protocol: validate(message)
                    result = await validator.validate(message)
                except TypeError:
                    # Fallback for legacy validators (expecting data in __init__)
                    validator = self.validator_class(message.to_dict())
                    result = await validator.validate()

                if result.has_errors():
                    from .exceptions import ValidationError

                    raise ValidationError(type(message).__name__, result.errors)
            return await handler_func(*args, **kwargs)

        return wrapped


class AuthorizationMiddleware(Middleware):
    """
    Middleware for checking permissions before execution.

    Note: This is a robust skeleton. Actual authorization logic (RBAC, ABAC)
    should be implemented by integrating with your security service or
    policy engine in the `apply` method.
    """

    def __init__(
        self,
        permissions: list = None,
        resource_type: str = None,
        resource_id_attr: str = None,
    ):
        self.permissions = permissions or []
        self.resource_type = resource_type
        self.resource_id_attr = resource_id_attr

    def apply(self, handler_func: Callable, message) -> Callable:
        async def wrapped(*args, **kwargs):
            # Authorization logic would integrate with your auth system
            # This is a placeholder - real implementation depends on your auth setup
            return await handler_func(*args, **kwargs)

        return wrapped


class LoggingMiddleware(Middleware):
    """
    Middleware for logging request execution and duration.

    Metadata extracted:
    - Command Name
    - Trace ID (Correlation ID)
    - Duration of execution

    Logs start, completion, and failures.
    """

    def __init__(self, level: str = "info"):
        self.level = level

    def apply(self, handler_func: Callable, message) -> Callable:
        logger = logging.getLogger("cqrs_ddd")

        async def wrapped(*args, **kwargs):
            command_name = type(message).__name__

            # Discover trace ID (correlation, command, or query)
            trace_id = getattr(message, "correlation_id", None)
            if not trace_id:
                trace_id = getattr(message, "command_id", None) or getattr(
                    message, "query_id", None
                )

            id_prefix = f"[{trace_id}] " if trace_id else ""

            logger.info(f"{id_prefix}Executing {command_name}")
            try:
                start_time = datetime.now()
                result = await handler_func(*args, **kwargs)
                duration = datetime.now() - start_time
                logger.info(
                    f"{id_prefix}Completed {command_name} in {duration.total_seconds():.3f}s"
                )
                return result
            except Exception as e:
                logger.error(f"{id_prefix}Failed {command_name}: {e}")
                raise

        return wrapped


class ThreadSafetyMiddleware(Middleware):
    """
    Middleware that ensures sequential execution locally via locking logic.

    Uses a pluggable `LockStrategy` (e.g., RedisLock, MemoryLock).
    Best used when multiple workers might access the same aggregate concurrently.
    """

    def __init__(
        self,
        entity_type: str,
        entity_id_attr: str,
        lock_strategy: Optional[LockStrategy] = None,
    ):
        self.entity_type = entity_type
        self.entity_id_attr = entity_id_attr
        self.lock_strategy = lock_strategy

    def apply(self, handler_func, command):
        """
        Wrap handler with lock acquisition/release.

        Algorithm:
        1. Extract entity ID from command using `entity_id_attr`.
        2. Acquire lock for `(entity_type, entity_id)`.
        3. Execute handler.
        4. Release lock (in finally block).
        """
        logger = logging.getLogger("cqrs_ddd")

        async def wrapped_handler(*args, **kwargs):
            strategy = self.lock_strategy

            # Legacy callable support
            if callable(strategy) and not hasattr(strategy, "acquire"):
                strategy = strategy()

            if not strategy:
                logger.warning(
                    "ThreadSafetyMiddleware: No lock strategy configured, skipping lock"
                )
                return await handler_func(*args, **kwargs)

            # Get entity ID(s)
            entity_id_or_ids = getattr(command, self.entity_id_attr, None)
            if entity_id_or_ids is None:
                raise ValueError(
                    f"Command must contain '{self.entity_id_attr}' attribute"
                )

            token = None
            try:
                logger.debug(
                    f"ThreadSafety: Acquiring lock for {self.entity_type}:{entity_id_or_ids}"
                )
                token = await strategy.acquire(self.entity_type, entity_id_or_ids)
                logger.debug(
                    f"ThreadSafety: Lock acquired for {self.entity_type}:{entity_id_or_ids}"
                )
                return await handler_func(*args, **kwargs)
            finally:
                if token:
                    logger.debug(
                        f"ThreadSafety: Releasing lock for {self.entity_type}:{entity_id_or_ids}"
                    )
                    await strategy.release(self.entity_type, entity_id_or_ids, token)

        return wrapped_handler


# =============================================================================
# Attribute Injector Protocol & Middleware
# =============================================================================
class AttributeInjector:
    """Protocol for attribute value injection."""

    async def compute(self, command: Any) -> Any:
        """Compute value to inject."""
        raise NotImplementedError


class AttributeInjectionMiddleware(Middleware):
    """
    Middleware that computes and injects attributes into the command before execution.

    Useful for:
    - Injecting Current User (from context)
    - Injecting Tenant ID
    - Injecting Request ID
    """

    def __init__(self, injectors: dict):
        self.injectors = injectors or {}

    def apply(self, handler_func, command):
        async def wrapped_handler(*args, **kwargs):
            for attr_name, injector in self.injectors.items():
                value = await injector.compute(command)
                setattr(command, attr_name, value)
            return await handler_func(*args, **kwargs)

        return wrapped_handler


# =============================================================================
# Event Store Persistence Middleware
# =============================================================================


class EventStorePersistenceMiddleware(Middleware):
    """
    Middleware that intercepts Command Results and persists Domain Events.

    Responsibilities:
    1. Ensures Correlation ID and Causation ID are present.
    2. Enriches events with metadata.
    3. Persists events to the EventStore (batch append).
    """

    def __init__(self, event_store: Optional[EventStore] = None):
        self._event_store = event_store

    def apply(self, handler_func, command):
        """
        Wrap handler with event persistence logic.

        If handler returns a result with `.events` list, this middleware
        persists them using the configured `EventStore`.
        """
        logger = logging.getLogger("cqrs_ddd")

        async def wrapped_handler(*args, **kwargs):
            # Step 1: Generate correlation_id
            from .event_store import generate_correlation_id

            correlation_id = getattr(command, "correlation_id", None)
            causation_id = getattr(command, "command_id", None)

            if not correlation_id:
                correlation_id = generate_correlation_id()
                try:
                    command.correlation_id = correlation_id
                except (AttributeError, TypeError):
                    pass

            # Step 2: Execute handler
            result = await handler_func(*args, **kwargs)

            # Step 2a: Propagate IDs
            if hasattr(result, "correlation_id") and not result.correlation_id:
                try:
                    result.correlation_id = correlation_id
                except (AttributeError, TypeError):
                    pass
            if hasattr(result, "causation_id") and not result.causation_id:
                try:
                    result.causation_id = causation_id
                except (AttributeError, TypeError):
                    pass

            # Step 3: Extract events
            events = []
            if hasattr(result, "events") and result.events:
                events = result.events

            # Step 4: Persist events
            if events:
                event_store = self._event_store

                if not event_store:
                    logger.warning(
                        "EventStorePersistenceMiddleware: No event store provided, skipping persistence"
                    )
                    return result

                from .domain_event import enrich_event_metadata

                enriched_events = [
                    enrich_event_metadata(
                        event, correlation_id=correlation_id, causation_id=causation_id
                    )
                    for event in events
                ]

                try:
                    logger.debug(
                        f"EventStorePersistenceMiddleware: Appending batch of {len(enriched_events)} events"
                    )
                    await event_store.append_batch(
                        enriched_events, correlation_id=correlation_id
                    )
                    logger.debug(
                        f"Persisted {len(enriched_events)} events with correlation_id={correlation_id}"
                    )
                except Exception as e:
                    logger.error(f"Failed to persist events: {e}")

            return result

        return wrapped_handler


class MiddlewareRegistry:
    """
    Registry for declarative middleware registration via decorators.

    Refactored to support Pluggable Middleware Classes (Strategy Pattern).
    Allows replacing default middleware implementations (e.g. for wired versions).
    """

    def __init__(self):
        self.classes = {
            "validator": ValidatorMiddleware,
            "authorization": AuthorizationMiddleware,
            "logging": LoggingMiddleware,
            "thread_safety": ThreadSafetyMiddleware,
            "attribute_injection": AttributeInjectionMiddleware,
            "event_store": EventStorePersistenceMiddleware,
        }

    def _register(self, handler_class, middleware_class: Type["Middleware"], **kwargs):
        if not hasattr(handler_class, "_middlewares"):
            handler_class._middlewares = []

        # Store definition for runtime instantiation
        handler_class._middlewares.append(
            MiddlewareDefinition(middleware_class=middleware_class, kwargs=kwargs)
        )
        return handler_class

    def validate(self, validator_class: Type = None):
        """
        Decorator to add validation middleware.

        Args:
            validator_class: A class implementing the validation protocol.
        """

        def decorator(handler_class):
            return self._register(
                handler_class,
                self.classes["validator"],
                validator_class=validator_class,
            )

        return decorator

    def authorize(
        self,
        permissions: list = None,
        resource_type: str = None,
        resource_id_attr: str = None,
    ):
        """
        Decorator to add authorization middleware.

        Args:
            permissions: List of required permissions.
            resource_type: Type of the resource being accessed.
            resource_id_attr: Attribute of the command containing the resource ID.
        """

        def decorator(handler_class):
            return self._register(
                handler_class,
                self.classes["authorization"],
                permissions=permissions,
                resource_type=resource_type,
                resource_id_attr=resource_id_attr,
            )

        return decorator

    def log(self, level: str = "info"):
        """Add logging middleware."""

        def decorator(handler_class):
            return self._register(handler_class, self.classes["logging"], level=level)

        return decorator

    def thread_safe(
        self,
        entity_type: str,
        entity_id_attr: str,
        lock_strategy: Optional[LockStrategy] = None,
    ):
        """
        Decorator to add thread safety middleware.

        Ensures sequential execution for commands targeting the same entity instance.

        Args:
            entity_type: The type of entity (e.g., 'User').
            entity_id_attr: attribute of the command to extract the ID from.
            lock_strategy: Optional custom lock strategy (overrides default).
        """

        def decorator(handler_class):
            kwargs = {"entity_type": entity_type, "entity_id_attr": entity_id_attr}
            if lock_strategy:
                kwargs["lock_strategy"] = lock_strategy

            return self._register(
                handler_class, self.classes["thread_safety"], **kwargs
            )

        return decorator

    def inject_attrs(self, injectors: dict):
        """Add attribute injection middleware."""

        def decorator(handler_class):
            return self._register(
                handler_class, self.classes["attribute_injection"], injectors=injectors
            )

        return decorator

    def persist_events(self, event_store: Optional[EventStore] = None):
        """
        Decorator to add event persistence middleware.

        Automatically persists any domain events in the command result to the event store.

        Args:
            event_store: Optional specific event store instance (overrides default).
        """

        def decorator(handler_class):
            kwargs = {}
            if event_store:
                kwargs["event_store"] = event_store
            return self._register(handler_class, self.classes["event_store"], **kwargs)

        return decorator

    def apply(self, middleware_class: Type[Middleware], *args, **kwargs):
        """
        Decorator to apply a custom middleware class.

        Args:
            middleware_class: The middleware class to instantiate.
            **kwargs: Arguments to pass to the middleware constructor.
        """

        def decorator(handler_class):
            return self._register(handler_class, middleware_class, **kwargs)

        return decorator


# Global middleware registry instance
middleware = MiddlewareRegistry()
