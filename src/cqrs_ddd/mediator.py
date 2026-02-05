import logging
from typing import Dict, Type, Union, Optional, Callable, Any
from contextvars import ContextVar

from .core import Command, Query, CommandResponse, AbstractCommand
from .protocols import UnitOfWork, EventDispatcher

logger = logging.getLogger(__name__)


# Context variable for current unit of work (enables nested commands)
_current_uow: ContextVar[Optional[UnitOfWork]] = ContextVar("current_uow", default=None)


def get_current_uow() -> Optional[UnitOfWork]:
    """Get the current unit of work from context (if any)."""
    return _current_uow.get()


class Mediator:
    """
    CQRS Mediator - routes commands and queries to their handlers.

    Commands automatically get UoW scope:
    - Root commands create a new UoW
    - Nested commands reuse parent's UoW (shared transaction)
    - Queries do not get UoW scope

    Usage:
        mediator = Mediator(uow_factory=lambda: SQLAlchemyUoW(session_factory))
        result = await mediator.send(CreateUser(name="Alice"))
    """

    def __init__(
        self,
        uow_factory: Optional[Callable[[], UnitOfWork]] = None,
        event_dispatcher: Optional[EventDispatcher] = None,
        handler_resolver: Optional[Callable[[Type], object]] = None,
        container: Any = None,
    ):
        """
        Initialize the mediator.

        Args:
            uow_factory: Factory function to create UnitOfWork instances
            event_dispatcher: Dispatcher for domain events
            handler_resolver: Optional function to resolve handlers (for DI integration)
            container: Dependency Injector Container (Preferred)
        """
        self._handlers: Dict[Type, object] = {}
        self._handler_resolver = handler_resolver
        self._container = None

        self._uow_factory = uow_factory
        self._event_dispatcher = event_dispatcher

        if container:
            self.set_container(container)

    def set_container(self, container: Any) -> None:
        """Set the DI container for handler resolution."""
        self._container = container

        # Resolve dependencies from container if not provided
        if not self._uow_factory and hasattr(container, "uow_factory"):
            self._uow_factory = container.uow_factory()

        if not self._event_dispatcher:
            # Check both core and direct for backward compatibility / flexibility
            core = getattr(container, "core", container)
            if hasattr(core, "event_dispatcher"):
                self._event_dispatcher = core.event_dispatcher()
                logger.debug("Mediator: event_dispatcher set from container")
            else:
                logger.warning(
                    "Mediator: No event_dispatcher set, background events will NOT be dispatched"
                )

    def register(self, message_type: Type, handler: object) -> None:
        """Register a handler for a message type."""
        self._handlers[message_type] = handler

    async def send(
        self, message: Union[Command, Query], uow: Optional[UnitOfWork] = None
    ) -> Any:
        """
        Send a command or query to its handler.

        Args:
            message: The command or query to send
            uow: Optional unit of work (for commands)

        Returns:
            The result from the handler
        """
        if isinstance(message, AbstractCommand):
            response = await self._send_command(message, uow)
        else:
            response = await self._dispatch(message)

        return response

    async def _send_command(
        self, command: Command, uow: Optional[UnitOfWork] = None
    ) -> Any:
        """
        Send a command with Unit of Work (transaction) management.

        Logic:
        1. Check if an active UoW exists (nested command).
           - If yes: reuse it, dispatch, propagate events.
           - If no: create new UoW, dispatch, commit/rollback, dispatch events.
        2. Execute middlewares chain.
        3. Dispatch priority events (synchronously, within transaction if typically).
        4. Dispatch background events (asynchronously, after commit).
        """
        handler = self._get_handler(type(command))

        # Ensure correlation_id exists for tracking
        from .domain_event import generate_correlation_id

        if not getattr(command, "correlation_id", None):
            try:
                command.correlation_id = generate_correlation_id()
            except (AttributeError, TypeError):
                pass

        existing_uow = _current_uow.get()

        if existing_uow:
            # Nested command - reuse parent's UoW
            response = await self._dispatch_with_middlewares(command, handler)
            self._propagate_ids(command, response)  # Early propagation
            await self._dispatch_events(response)
            return response

        # Root command - create UoW scope
        uow = uow or (self._uow_factory() if self._uow_factory else None)

        if uow:
            async with uow:
                token = _current_uow.set(uow)
                try:
                    response = await self._dispatch_with_middlewares(command, handler)
                    self._propagate_ids(command, response)  # Early propagation

                    # Flush to detect DB errors before event dispatch
                    if hasattr(uow, "session"):
                        await uow.session.flush()

                    # Priority events - in transaction
                    await self._dispatch_priority_events(response)
                finally:
                    _current_uow.reset(token)

            # Background events - after commit
            if not self._event_dispatcher:
                logger.warning(
                    "Mediator: No event_dispatcher set, background events will NOT be dispatched"
                )
            await self._dispatch_background_events(response)
        else:
            response = await self._dispatch_with_middlewares(command, handler)
            self._propagate_ids(command, response)  # Early propagation
            await self._dispatch_events(response)

        return response

    def _propagate_ids(self, message: Any, response: Any) -> None:
        """Propagate correlation ID and causation ID from command/query to response."""
        # Check if response supports IDs via hasattr (supports both dataclasses and Pydantic)
        if hasattr(response, "correlation_id"):
            if not getattr(response, "correlation_id", None):
                setattr(
                    response, "correlation_id", getattr(message, "correlation_id", None)
                )

        if hasattr(response, "causation_id"):
            if not getattr(response, "causation_id", None):
                setattr(
                    response,
                    "causation_id",
                    getattr(message, "command_id", None)
                    or getattr(message, "query_id", None),
                )

    async def _dispatch(self, message: Union[Command, Query]) -> Any:
        """
        Dispatch a message to its handler (without UoW management).

        Used primarily for Queries, or Commands where UoW is managed externally.
        """
        handler = self._get_handler(type(message))
        response = await self._dispatch_with_middlewares(message, handler)
        self._propagate_ids(message, response)
        return response

    async def _dispatch_with_middlewares(
        self, message: Union[Command, Query], handler: object
    ) -> Any:
        """
        Execute the handler wrapped in its middleware chain.

        Middlewares are applied in reverse order (stack-like), so the first
        defined middleware is the first to execute.
        """
        middlewares = getattr(handler.__class__, "_middlewares", [])

        async def execute_handler(msg):
            return await handler.handle(msg)

        # Apply middlewares in registration order (Bottom-to-Top)
        # to ensure the top-most decorator remains the outermost layer.
        handler_func = execute_handler

        # Import definition here to avoid circular import if top-level
        from .middleware import MiddlewareDefinition

        for middleware_item in middlewares:
            middleware_instance = None

            # Pattern B: Runtime instantiation from definition
            if isinstance(middleware_item, MiddlewareDefinition):
                # Instantiate (this triggers @inject to wire dependencies from active container)
                try:
                    middleware_instance = middleware_item.middleware_class(
                        **middleware_item.kwargs
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to instantiate middleware {middleware_item.middleware_class}: {e}"
                    )
                    raise
            # Legacy/Manual Pattern: Pre-instantiated middleware
            else:
                middleware_instance = middleware_item

            handler_func = middleware_instance.apply(handler_func, message)

        return await handler_func(message)

    def _get_handler(self, message_type: Type) -> object:
        """Get handler for a message type."""
        handler = self._handlers.get(message_type)

        if not handler:
            # Lazy Discovery: Check decorators registry
            from .handler_registry import get_registered_handlers

            registered = get_registered_handlers()

            handler_cls = registered["commands"].get(message_type) or registered[
                "queries"
            ].get(message_type)

            if handler_cls:
                try:
                    # In Zero Config mode, we try to instantiate the handler.
                    # If it uses @inject, it needs to be wired to a container.
                    handler = handler_cls()
                    # Cache for future use
                    self._handlers[message_type] = handler
                except Exception as e:
                    logger.debug(
                        f"Direct instantiation of {handler_cls.__name__} failed (expected if it has dependencies): {e}"
                    )
                    # Fall through to resolver/container logic
                    pass

        if not handler and self._handler_resolver:
            handler = self._handler_resolver(message_type)

        if not handler:
            raise ValueError(f"No handler registered for {message_type.__name__}")

        # If it's a provider (callable returning object), instantiate it
        if (
            callable(handler)
            and not isinstance(handler, type)
            and not hasattr(handler, "handle")
        ):
            # It might be a dependency_injector Provider
            if hasattr(handler, "provide"):  # rudimentary check
                handler = handler()

        return handler

    async def _dispatch_events(self, response: CommandResponse) -> None:
        """
        Dispatch all events found in the CommandResponse.

        Delegates to _dispatch_priority_events and _dispatch_background_events.
        """
        await self._dispatch_priority_events(response)
        await self._dispatch_background_events(response)

    async def _dispatch_priority_events(self, response: Any) -> None:
        """Dispatch priority events synchronously."""
        if not self._event_dispatcher:
            return

        events = getattr(response, "events", None)
        if not events:
            return

        from .domain_event import enrich_event_metadata

        correlation_id = getattr(response, "correlation_id", None)
        causation_id = getattr(response, "causation_id", None)

        for i, event in enumerate(events):
            enriched_event = enrich_event_metadata(
                event, correlation_id=correlation_id, causation_id=causation_id
            )
            if hasattr(response, "events") and isinstance(response.events, list):
                response.events[i] = enriched_event
            # Update the event in the response list with the enriched metadata
            # (correlation_id, causation_id, etc.)
            await self._event_dispatcher.dispatch_priority(enriched_event)

    async def _dispatch_background_events(self, response: Any) -> None:
        """Dispatch background events asynchronously."""
        if not self._event_dispatcher:
            return

        events = getattr(response, "events", None)
        if not events:
            return

        from .domain_event import enrich_event_metadata

        correlation_id = getattr(response, "correlation_id", None)
        causation_id = getattr(response, "causation_id", None)

        for i, event in enumerate(events):
            enriched_event = enrich_event_metadata(
                event, correlation_id=correlation_id, causation_id=causation_id
            )
            # update back if possible
            if hasattr(response, "events") and isinstance(response.events, list):
                response.events[i] = enriched_event

            await self._event_dispatcher.dispatch_background(enriched_event)

    def clear_handlers(self) -> None:
        """Clear all registered handlers."""
        self._handlers.clear()
