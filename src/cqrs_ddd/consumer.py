"""Base Event Consumer implementation."""
import logging
import asyncio
from typing import List, Optional, Any, Callable

from .protocols import MessageConsumer, EventDispatcher, EventConsumer
from .event_registry import EventTypeRegistry

logger = logging.getLogger("cqrs_ddd")


class BaseEventConsumer(EventConsumer):
    """
    Base implementation of an EventConsumer.
    
    Subscribes to one or more topics on a MessageBroker.
    When a message is received:
    1. Hydrates the payload using EventTypeRegistry
    2. Dispatches the hydrated event using EventDispatcher (background)
    """

    def __init__(
        self,
        broker: MessageConsumer,
        dispatcher: EventDispatcher,
        topics: List[str],
        queue_name: Optional[str] = None
    ):
        """
        Initialize the consumer.
        
        Args:
            broker: The message broker to subscribe to
            dispatcher: The event dispatcher to send hydrated events to
            topics: List of topics/routing keys to subscribe to
            queue_name: Optional explicit queue name for group consumption
        """
        self.broker = broker
        self.dispatcher = dispatcher
        self.topics = topics
        self.queue_name = queue_name
        self._running = False

    async def start(self) -> None:
        """Start subscribing and listening for messages."""
        if self._running:
            return

        self._running = True
        logger.info(f"Starting EventConsumer for topics: {self.topics}")
        
        for topic in self.topics:
            await self.broker.subscribe(
                topic=topic,
                handler=self._handle_message,
                queue_name=self.queue_name
            )
        
        logger.info("EventConsumer started and subscribed to all topics")

    async def stop(self) -> None:
        """Stop listening (implementation depends on broker capabilities)."""
        self._running = False
        logger.info("EventConsumer stopped")

    async def _handle_message(self, payload: Any) -> None:
        """
        Internal handler for messages from the broker.
        
        Payload is expected to be a dict with 'event_type' and properties.
        """
        if not isinstance(payload, dict):
            logger.warning(f"Consumer received non-dict payload: {type(payload)}")
            return

        event_type = payload.get("event_type")
        if not event_type:
            logger.warning("Consumer received payload without 'event_type'")
            return

        try:
            # Hydrate event
            event = EventTypeRegistry.hydrate_dict(event_type, payload)
            
            if not event:
                logger.warning(f"Failed to hydrate event of type '{event_type}'. Is it registered?")
                return

            # Dispatch locally
            logger.debug(f"Consumer hydrated and dispatching {event_type}")
            await self.dispatcher.dispatch_background(event)
            
        except Exception as e:
            logger.error(f"Error in EventConsumer while processing {event_type}: {e}", exc_info=True)
