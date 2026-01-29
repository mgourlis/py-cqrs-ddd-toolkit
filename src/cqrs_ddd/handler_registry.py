"""
Registry for CQRS/DDD handlers.

This module maintains the mapping between:
- Commands and their CommandHandlers
- Queries and their QueryHandlers
- Domain Events and their EventHandlers (Priority & Background)

It is used by the auto-registration mechanism in `core.py` and the `Mediator` 
to dispatch messages to the correct handlers.
"""
from typing import Type, Dict, List, Any
import logging

# Registry Storage
_command_handlers: Dict[Type, Type] = {}
_query_handlers: Dict[Type, Type] = {}
_event_handlers: Dict[Type, Dict[str, List[Type]]] = {}

logger = logging.getLogger("cqrs_ddd")


def register_command_handler(command_type: Type, handler_class: Type) -> None:
    """
    Register a command handler for a specific command type.
    
    Args:
        command_type: The command class (dataclass/pydantic model)
        handler_class: The handler class that implements `handle(command)`
    """
    if not command_type:
        name = handler_class.__name__ if handler_class else "Unknown"
        logger.warning(f"Failed to register command handler for {name}: command_type is None")
        return
    _command_handlers[command_type] = handler_class
    logger.debug(f"Registered CommandHandler {handler_class.__name__} for {command_type.__name__}")


def register_query_handler(query_type: Type, handler_class: Type) -> None:
    """
    Register a query handler for a specific query type.
    
    Args:
        query_type: The query class
        handler_class: The handler class that implements `handle(query)`
    """
    if not query_type:
        name = handler_class.__name__ if handler_class else "Unknown"
        logger.warning(f"Failed to register query handler for {name}: query_type is None")
        return
    _query_handlers[query_type] = handler_class
    logger.debug(f"Registered QueryHandler {handler_class.__name__} for {query_type.__name__}")


def register_event_handler(event_type: Type, handler_class: Type, priority: bool = False) -> None:
    """
    Register an event handler for a specific domain event.
    
    Args:
        event_type: The domain event class
        handler_class: The handler class
        priority: If True, this handler runs synchronously in the main transaction.
                 If False, it runs in the background (after transaction commit).
    """
    if not event_type:
        name = handler_class.__name__ if handler_class else "Unknown"
        logger.warning(f"Failed to register event handler for {name}: event_type is None")
        return
    
    if event_type not in _event_handlers:
        _event_handlers[event_type] = {'priority': [], 'background': []}
    
    if priority:
        _event_handlers[event_type]['priority'].append(handler_class)
    else:
        _event_handlers[event_type]['background'].append(handler_class)
    
    type_kind = "priority" if priority else "background"
    logger.debug(f"Registered EventHandler {handler_class.__name__} for {event_type.__name__} ({type_kind})")


def get_registered_handlers() -> Dict[str, Any]:
    """
    Get a snapshot of all registered handlers.
    
    Returns:
        Dict containing copies of 'commands', 'queries', and 'events' registries.
        Useful for debugging or introspection.
    """
    return {
        'commands': _command_handlers.copy(),
        'queries': _query_handlers.copy(),
        'events': _event_handlers.copy(),
    }
