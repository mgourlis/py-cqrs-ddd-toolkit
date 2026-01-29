"""Domain Event definitions."""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Type
import uuid

from .core import AbstractDomainEvent

# =============================================================================
# Helper Functions
# =============================================================================

def generate_event_id() -> str:
    """
    Generate unique event ID.
    
    Returns:
        A UUID4 string.
    """
    return str(uuid.uuid4())


def generate_correlation_id() -> str:
    """
    Generate unique correlation ID for grouping related events.
    
    Returns:
        A UUID4 string.
    """
    return str(uuid.uuid4())


def enrich_event_metadata(
    event: Any,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
    user_id: Optional[str] = None
) -> Any:
    """
    Safely enrich an event with metadata, supporting both mutable and frozen objects.
    
    If the object is frozen (like Pydantic models), it returns a copy via model_copy().
    """
    updates = {}
    if correlation_id and hasattr(event, 'correlation_id') and not getattr(event, 'correlation_id'):
        updates['correlation_id'] = correlation_id
    if causation_id and hasattr(event, 'causation_id') and not getattr(event, 'causation_id'):
        updates['causation_id'] = causation_id
    if user_id and hasattr(event, 'user_id') and not getattr(event, 'user_id'):
        updates['user_id'] = user_id
        
    if not updates:
        return event
        
    try:
        # Try inplace mutation first
        for key, value in updates.items():
            setattr(event, key, value)
        return event
    except (AttributeError, TypeError, Exception):
        # Handle frozen object by creating a copy
        # Try Pydantic v2 model_copy
        if hasattr(event, 'model_copy'):
            return event.model_copy(update=updates)
        # Try Pydantic v1 copy
        elif hasattr(event, 'copy'):
            return event.copy(update=updates)
        # Fallback: just return original if we can't enrich
        return event


# =============================================================================
# Domain Event Base Class
# =============================================================================

@dataclass
class DomainEventBase(AbstractDomainEvent):
    """
    Enhanced base class for domain events with event store support.
    
    Features:
    - Unique event ID for tracking
    - Timestamp for ordering
    - User tracking for audit
    - Correlation ID for grouping related events (e.g., cascade operations)
    - Causation ID for tracking event chains
    - Version for schema evolution
    
    Subclasses should:
    1. Define their specific fields
    2. Implement aggregate_id property
    3. Implement to_dict() and from_dict() for serialization
    
    Events are automatically registered with EventTypeRegistry on subclass definition.
    """
    
    def __init_subclass__(cls, **kwargs):
        """Auto-register event classes with EventTypeRegistry."""
        super().__init_subclass__(**kwargs)
        # Lazy import to avoid circular dependency
        from .event_registry import EventTypeRegistry
        EventTypeRegistry.register_class(cls)
    
    # Auto-generated event ID
    event_id: str = field(default_factory=generate_event_id)
    
    # When the event occurred
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Who triggered the event
    user_id: Optional[str] = None
    
    # Groups related events (e.g., all events from one command)
    correlation_id: Optional[str] = None
    
    # Parent event that caused this event (for cascade tracking)
    causation_id: Optional[str] = None
    
    # Event schema version for evolution
    version: int = 1
    
    @property
    def event_type(self) -> str:
        """Return event type name for storage."""
        return self.__class__.__name__
    
    @property
    def aggregate_type(self) -> str:
        """Return aggregate type name. Override in subclass."""
        return "Unknown"
    
    @property
    def aggregate_id(self) -> Any:
        """Return the aggregate ID this event belongs to. Override in subclass."""
        raise NotImplementedError("Subclasses must implement aggregate_id")
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize event for storage."""
        return {
            "event_id": self.event_id,
            "occurred_at": self.occurred_at.isoformat() if self.occurred_at else None,
            "user_id": self.user_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "version": self.version,
            "event_type": self.event_type,
            "aggregate_type": self.aggregate_type,
            "aggregate_id": self.aggregate_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DomainEventBase':
        """Deserialize event from storage. Override in subclass."""
        raise NotImplementedError("Subclasses must implement from_dict()")



