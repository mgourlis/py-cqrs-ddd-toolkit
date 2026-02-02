"""Event Type Registry for event hydration.

Maps event type strings to event classes, enabling reconstruction
of domain events from stored payloads (StoredEvent -> DomainEvent).
"""

from typing import Dict, Type, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .event_store import StoredEvent


class EventTypeRegistry:
    """
    Registry for mapping event type strings to event classes.

    Used by the UndoService to hydrate stored events back into
    domain event objects for undo/redo operations.

    Usage:
        # Events auto-register via __init_subclass__
        class ProductPriceUpdated(DomainEventBase):
            ...

        # Or register manually
        EventTypeRegistry.register_class(ProductPriceUpdated)

        # Hydrate stored event
        event = EventTypeRegistry.hydrate(stored_event)
    """

    _registry: Dict[str, Type] = {}  # Type hint simplified to avoid import

    @classmethod
    def register(cls, event_class: Type) -> Type:
        """
        Decorator to register an event class.

        Args:
            event_class: The domain event class to register

        Returns:
            The event class (unchanged, for decorator use)
        """
        cls._registry[event_class.__name__] = event_class
        return event_class

    @classmethod
    def register_class(cls, event_class: Type) -> None:
        """
        Register an event class (alternative to decorator).

        Args:
            event_class: The domain event class to register
        """
        cls._registry[event_class.__name__] = event_class

    @classmethod
    def get(cls, event_type: str) -> Optional[Type]:
        """
        Get event class by type name.

        Args:
            event_type: The event type string (class name)

        Returns:
            The event class, or None if not registered
        """
        return cls._registry.get(event_type)

    @classmethod
    def has(cls, event_type: str) -> bool:
        """Check if an event type is registered."""
        return event_type in cls._registry

    @classmethod
    def hydrate(cls, stored_event: "StoredEvent") -> Optional[Any]:
        """
        Reconstruct domain event from stored event.

        Args:
            stored_event: The stored event with payload

        Returns:
            Hydrated domain event, or None if event type not registered
        """
        event_class = cls.get(stored_event.event_type)
        if event_class is None:
            return None

        # Use from_dict if available (for proper deserialization)
        if hasattr(event_class, "from_dict"):
            return event_class.from_dict(stored_event.payload)

        # Fallback: try direct instantiation from payload
        try:
            return event_class(**stored_event.payload)
        except TypeError:
            return None

    @classmethod
    def hydrate_dict(cls, event_type: str, payload: Dict[str, Any]) -> Optional[Any]:
        """
        Reconstruct domain event from event type and payload dict.

        Args:
            event_type: The event type string
            payload: The event payload dictionary

        Returns:
            Hydrated domain event, or None if event type not registered
        """
        event_class = cls.get(event_type)
        if event_class is None:
            return None

        if hasattr(event_class, "from_dict"):
            return event_class.from_dict(payload)

        try:
            return event_class(**payload)
        except TypeError:
            return None

    @classmethod
    def list_registered(cls) -> list:
        """List all registered event types."""
        return list(cls._registry.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registrations (for testing)."""
        cls._registry.clear()
