"""Event Dispatcher for domain events."""

import asyncio
import logging
from typing import Dict, Type, List, Any

logger = logging.getLogger("cqrs_ddd")


class EventDispatcher:
    """
    Event dispatcher for domain events.

    Supports two types of handlers:
    - Priority handlers: Awaited synchronously before command returns
    - Background handlers: Fire-and-forget after transaction commit

    Usage:
        dispatcher = EventDispatcher()
        dispatcher.register(UserCreated, NotifyNewUser())
        dispatcher.register(UserCreated, SyncToAuthSystem(), priority=True)

        await dispatcher.dispatch_priority(event)  # In transaction
        await dispatcher.dispatch_background(event)  # After commit
    """

    def __init__(self, max_concurrent: int = 20):
        """
        Initialize the dispatcher.

        Args:
            max_concurrent: Maximum concurrent background handlers
        """
        self._subscribers: Dict[Type, List[Any]] = {}
        self._priority_subscribers: Dict[Type, List[Any]] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)

    def register(
        self, event_type: Type, handler: object, priority: bool = False
    ) -> None:
        """
        Register an event handler.

        Args:
            event_type: The event type to subscribe to
            handler: Handler instance with async handle() method
            priority: If True, handler runs synchronously in transaction
        """
        target = self._priority_subscribers if priority else self._subscribers

        if event_type not in target:
            target[event_type] = []

        if handler not in target[event_type]:
            target[event_type].append(handler)
            priority_tag = " [PRIORITY]" if priority else ""
            logger.debug(f"Registered handler for {event_type.__name__}{priority_tag}")

    async def dispatch_priority(self, event: Any) -> None:
        """
        Dispatch to priority handlers synchronously.

        These handlers run:
        - Inside the UoW transaction.
        - Before the transaction commits.

        Use Cases:
        - Updating read models (if strong consistency required).
        - Validation logic that depends on event.
        - Synchronous external calls (use with caution).

        If a priority handler fails, the exception propagates and the UoW rolls back.
        """
        event_type = type(event)
        self._discover_handlers(event_type)
        handlers = self._priority_subscribers.get(event_type, [])

        for handler in handlers:
            try:
                await self._run_handler(handler, event)
            except Exception as e:
                logger.error(
                    f"Priority handler {handler} failed for {event_type.__name__}: {e}",
                    exc_info=True,
                )
                raise  # Priority handlers must succeed

    async def dispatch_background(self, event: Any) -> None:
        """
        Dispatch to background handlers asynchronously.

        These handlers run:
        - As fire-and-forget asyncio Tasks.
        - Outside the command's transaction (after commit).

        Concurrency is limited by `max_concurrent` semaphore.
        Errors are logged but do NOT affect the command response.
        """
        event_type = type(event)
        self._discover_handlers(event_type)
        handlers = self._subscribers.get(event_type, [])

        for handler in handlers:
            asyncio.create_task(self._run_handler_safe(handler, event))

    def _discover_handlers(self, event_type: Type) -> None:
        """Discover and instantiate handlers from the registry."""
        from .handler_registry import get_registered_handlers

        registered_events = get_registered_handlers().get("events", {})

        handlers_meta = registered_events.get(event_type)
        if not handlers_meta:
            return

        # Register priority handlers
        for handler_cls in handlers_meta.get("priority", []):
            # Check if an instance of this class is already registered
            if not any(
                isinstance(h, handler_cls)
                for h in self._priority_subscribers.get(event_type, [])
            ):
                try:
                    self.register(event_type, handler_cls(), priority=True)
                except Exception as e:
                    logger.error(
                        f"Failed to instantiate priority handler {handler_cls.__name__}: {e}"
                    )

        # Register background handlers
        for handler_cls in handlers_meta.get("background", []):
            if not any(
                isinstance(h, handler_cls)
                for h in self._subscribers.get(event_type, [])
            ):
                try:
                    self.register(event_type, handler_cls(), priority=False)
                except Exception as e:
                    logger.error(
                        f"Failed to instantiate background handler {handler_cls.__name__}: {e}"
                    )

    async def dispatch(self, event: Any) -> None:
        """
        Dispatch to both priority and background handlers.

        Deprecated: Use dispatch_priority and dispatch_background separately
        for better control over when events are processed.
        """
        await self.dispatch_priority(event)
        await self.dispatch_background(event)

    async def _run_handler(self, handler: Any, event: Any) -> None:
        """Run handler with concurrency limiting via semaphore."""
        async with self._semaphore:
            await handler.handle(event)

    async def _run_handler_safe(self, handler: Any, event: Any) -> None:
        """Run handler with exception handling for background tasks."""
        try:
            await self._run_handler(handler, event)
        except Exception as e:
            logger.error(
                f"Background handler {handler} failed for {type(event).__name__}: {e}",
                exc_info=True,
            )

    def clear_subscribers(self) -> None:
        """Clear all event subscribers."""
        self._subscribers.clear()
        self._priority_subscribers.clear()
