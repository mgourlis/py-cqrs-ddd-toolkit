"""Registry for Saga classes and their event handlers."""
from typing import Dict, List, Type, TypeVar, Optional
import logging

logger = logging.getLogger("cqrs_ddd")

T_Saga = TypeVar("T_Saga", bound="Saga")

class SagaRegistry:
    """
    Global registry for Saga classes.
    
    Maps DomainEvent types to the Saga classes that handle them.
    Allows for automatic discovery of Sagas by the SagaManager.
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SagaRegistry, cls).__new__(cls)
            cls._instance._event_map = {}
            cls._instance._type_map = {}
        return cls._instance
    
    def register_type(self, saga_class: Type):
        """Register a saga class by name."""
        self._type_map[saga_class.__name__] = saga_class
        logger.debug(f"Registered Saga type {saga_class.__name__}")

    def get_saga_type(self, name: str) -> Optional[Type]:
        """Get saga class by name."""
        return self._type_map.get(name)
    
    def register(self, event_type: Type, saga_class: Type):
        """
        Register a saga class for an event type.
        
        Args:
            event_type: The domain event class to handle.
            saga_class: The Saga subclass to invoke.
        """
        if event_type not in self._event_map:
            self._event_map[event_type] = []
        
        if saga_class not in self._event_map[event_type]:
            self._event_map[event_type].append(saga_class)
            logger.debug(f"Registered Saga {saga_class.__name__} for event {event_type.__name__}")
            
    def get_sagas_for_event(self, event_type: Type) -> List[Type]:
        """Get all saga classes registered for an event type."""
        return self._event_map.get(event_type, [])
    
    def clear(self):
        """Clear all registrations."""
    def clear(self):
        """Clear all registrations."""
        self._event_map = {}
        self._type_map = {}

# Global instance
saga_registry = SagaRegistry()
