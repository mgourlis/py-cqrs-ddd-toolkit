"""
Event Handlers for Publishing Events.

This module provides generic handlers that can be registered with the
EventDispatcher to explicitly publish events to external systems
(via Outbox or direct Broker).
"""
import logging
from typing import Any, Dict, Optional

from .protocols import MessagePublisher

logger = logging.getLogger("cqrs_ddd.publishers")


class PublishingEventHandler:
    """
    Generic event handler that publishes events to a configured publisher.
    
    Usage:
        # 1. Create the handler with your publisher (e.g., OutboxService)
        publisher_handler = PublishingEventHandler(publisher=outbox_service)
        
        # 2. Register it for specific events
        dispatcher.register(UserCreated, publisher_handler)
        dispatcher.register(OrderPlaced, publisher_handler)
    """
    
    def __init__(self, publisher: MessagePublisher):
        self.publisher = publisher
        
    async def handle(self, event: Any) -> None:
        """
        Handle the event by publishing it.
        
        The topic is derived from the event class name.
        """
        topic = type(event).__name__
        try:
            logger.debug(f"Publishing event {topic} via {self.publisher}")
            await self.publisher.publish(topic, event)
        except Exception as e:
            logger.error(
                f"Failed to publish event {topic} using {self.publisher}: {e}", 
                exc_info=True
            )
            # We re-raise to ensure the background task machinery knows it failed
            raise


# Registry for mapping Event Class Names -> Destination Keys
# Registry for mapping Event Class Names -> Destination Keys
EVENT_DESTINATIONS: Dict[str, str] = {}
# Registry for keeping track of definition-time routed classes
ROUTED_EVENT_CLASSES: list = []


def route_to(destination: str):
    """
    Decorator to register a Domain Event to a specific destination key.
    
    Usage:
        @route_to("dramatiq")
        class UserCreated(DomainEvent): ...
    """
    def wrapper(cls):
        EVENT_DESTINATIONS[cls.__name__] = destination
        ROUTED_EVENT_CLASSES.append(cls)
        return cls
    return wrapper


def auto_register_routed_events(dispatcher: Any, publisher: Any) -> int:
    """
    Automatically register all events decorated with @route_to to the given publisher.
    
    Args:
        dispatcher: The EventDispatcher instance.
        publisher: The handler/publisher (usually PublishingEventHandler).
        
    Returns:
        Count of events registered.
    """
    count = 0
    for cls in ROUTED_EVENT_CLASSES:
        # Avoid double registration if user already did it manually?
        # Dispatcher usually handles overrides, so it's safe.
        dispatcher.register(cls, publisher)
        count += 1
    logger.info(f"Auto-registered {count} events tagged with @route_to")
    return count


class TopicRoutingPublisher(MessagePublisher):
    """
    Routes messages based on Topic OR Destination Key.
    
    Resolves destination in this order:
    1. Check if 'topic' has a registered destination key (via @route_to).
    2. Check if 'topic' is directly mapped in 'routes'.
    3. Use 'default' publisher.
    
    Usage:
        router = TopicRoutingPublisher(
            destinations={
                "dramatiq": dramatiq_pub,
                "rabbit": rabbit_pub
            },
            default=rabbit_pub
        )
    """
    
    def __init__(
        self, 
        routes: Optional[Dict[str, MessagePublisher]] = None, 
        destinations: Optional[Dict[str, MessagePublisher]] = None,
        default: Optional[MessagePublisher] = None
    ):
        self.routes = routes or {}
        self.destinations = destinations or {}
        self.default = default

    def register(self, topic: str, publisher: MessagePublisher) -> None:
        """Register a specific publisher for a topic string."""
        self.routes[topic] = publisher
        logger.debug(f"Registered route: {topic} -> {type(publisher).__name__}")
        
    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """Route the publication to the configured publisher."""
        publisher = None
        
        # 1. Check for auto-registered destination key (from @route_to)
        dest_key = EVENT_DESTINATIONS.get(topic)
        if dest_key:
            publisher = self.destinations.get(dest_key)
            # logger.debug(f"Resolved auto-route: {topic} -> {dest_key} -> {publisher}")

        # 2. Check direct topic route (overrides auto-route if present?)
        # Explicit route overrides auto-route
        if topic in self.routes:
            publisher = self.routes[topic]
            
        # 3. Fallback to default
        if not publisher:
            publisher = self.default
        
        if not publisher:
            logger.warning(f"No publisher configured for topic '{topic}' and no default provided. Message dropped.")
            return

        await publisher.publish(topic, message, **kwargs)
