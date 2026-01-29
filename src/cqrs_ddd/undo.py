"""Undo/Redo Infrastructure."""
from abc import ABC
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic
import logging



from .domain_event import (
   DomainEventBase, 
   generate_correlation_id
)
from .protocols import (
    UndoExecutor as UndoExecutorProtocol, 
    UndoService,
    EventStore,
    UndoExecutorRegistry
)
from .persistence_dispatcher import PersistenceDispatcher
from .event_store import StoredEvent
from .event_registry import EventTypeRegistry

logger = logging.getLogger("cqrs_ddd")

T = TypeVar('T')

# =============================================================================
# Undo/Redo Data Classes
# =============================================================================

@dataclass 
class UndoableAction:
    """Represents an action that can be undone."""
    
    event_id: str
    correlation_id: Optional[str]
    event_type: str
    description: str
    occurred_at: datetime
    user_id: Optional[str]
    can_undo: bool = True
    causation_id: Optional[str] = None
    aggregate_type: str = ""
    aggregate_id: Any = None


@dataclass
class UndoResult:
    """Result of an undo operation."""
    
    success: bool
    undone_events: List[str]  # Event IDs that were undone
    new_events: List[str]     # Event IDs of compensating events
    domain_events: List['DomainEventBase'] = field(default_factory=list) # Actual event objects
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    errors: List[str] = field(default_factory=list)


@dataclass
class RedoResult:
    """Result of a redo operation."""
    
    success: bool
    redone_events: List[str]
    domain_events: List['DomainEventBase'] = field(default_factory=list) # Actual event objects
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    errors: List[str] = field(default_factory=list)


# =============================================================================
# Undo Executor Infrastructure
# =============================================================================

# Global registry map (EventType -> Executor Class)
_EXECUTOR_CLASSES: Dict[str, Type['UndoExecutor']] = {}

class UndoExecutor(UndoExecutorProtocol[T], ABC):
    """
    Base class for undo executors with auto-registration and DI support.
    
    Attributes:
        dispatcher: PersistenceDispatcher for data access
        mediator: Mediator for dispatching commands/queries
    """
    
    _persistence_dispatcher: Optional[PersistenceDispatcher] = None
    _mediator: Optional[Any] = None # Avoid circular import of Mediator
    
    def __init__(
        self, 
        persistence_dispatcher: PersistenceDispatcher = None,
        mediator: Any = None
    ):
        self._persistence_dispatcher = persistence_dispatcher
        self._mediator = mediator
        
    @property
    def persistence_dispatcher(self) -> PersistenceDispatcher:
        """Get the injected dispatcher."""
        if self._persistence_dispatcher:
            return self._persistence_dispatcher
        raise RuntimeError(
            "PersistenceDispatcher not injected. "
            "Use Dependency Injection to provide it in __init__."
        )
        
    @property
    def mediator(self) -> Any:
        """Get the injected mediator."""
        return self._mediator

    @property
    def event_type(self) -> str:
        """
        The event type this executor handles.
        
        Automatically determined from the Generic type argument if not overridden.
        e.g. UndoExecutor[ProductCreated] -> "ProductCreated"
        """
        if hasattr(self, '_event_type'):
            return self._event_type
        return self.__class__.__name__.replace("UndoExecutor", "")

    def __init_subclass__(cls, **kwargs):
        """Auto-register executor based on Generic type."""
        super().__init_subclass__(**kwargs)
        
        # Determine event type from Generic[T] 
        event_type_name = None
        
        try:
             import typing
             for base in getattr(cls, "__orig_bases__", []):
                 origin = typing.get_origin(base)
                 # Check if origin is UndoExecutor (or has same name/module to be safe against import aliasing)
                 if origin is UndoExecutor or (
                     getattr(origin, '__name__', '') == 'UndoExecutor' 
                     and 'undo' in getattr(origin, '__module__', '')
                 ):
                     args = typing.get_args(base)
                     if args and hasattr(args[0], "__name__"):
                         event_type_name = args[0].__name__
                         break

        except Exception as e:
            # Silent failure during introspection is acceptable
            pass
            
        # Register in global registry
        et = None
        if hasattr(cls, '_event_type'):
            et = cls._event_type
        # Fallback via name convention if property not overridden on class
        elif 'event_type' not in cls.__dict__:
             # If event_type is just inherited from base, we rely on __name__
             et = cls.__name__.replace("UndoExecutor", "")
             # If introspection found something different, prefer that?
             # Actually, Generic type is most reliable source of truth if convention fails
             if event_type_name and event_type_name not in et:
                 # Convention might be ProductCreatedUndoExecutor -> ProductCreated
                 pass
        
        if not et and event_type_name:
            et = event_type_name
            
        if et:
            _EXECUTOR_CLASSES[et] = cls
            logger.debug(f"Registered UndoExecutor {cls.__name__} for event {et}")
        else:
            logger.warning(f"Could not determine event type for UndoExecutor {cls.__name__}")


class DefaultUndoExecutorRegistry:
    """Registry of undo executors by event type."""
    
    def __init__(self):
        """
        Initialize registry.
        
        Uses the global _EXECUTOR_CLASSES map populated by UndoExecutor.__init_subclass__.
        """
        pass
    
    def register(self, executor: 'UndoExecutor') -> None:
        """Register an undo executor instance (legacy/manual)."""
        pass
    
    def get(self, event_type: str) -> Optional['UndoExecutor']:
        """Get executor for an event type."""
        executor_cls = _EXECUTOR_CLASSES.get(event_type)
        if executor_cls:
            # Instantiate on demand (DI will handle dependencies via @inject)
            return executor_cls()
        return None
    
    def has_executor(self, event_type: str) -> bool:
        """
        Check if an executor exists for an event type.
        
        Args:
            event_type: Name of the event type.
            
        Returns:
            True if an executor is registered.
        """
        return event_type in _EXECUTOR_CLASSES

# DefaultUndoService Implementation

class DefaultUndoService(UndoService):
    """
    Production-ready implementation of UndoService.
    
    Manages undo/redo operations by:
    1. Fetching events from the event store
    2. Hydrating stored events to domain events via EventTypeRegistry
    3. Executing undo via registered UndoExecutors
    4. Persisting compensating events and marking originals as undone
    """
    
    def __init__(
        self,
        event_store: EventStore,
        executor_registry: UndoExecutorRegistry,
        cache_service: Optional[Any] = None
    ):
        self.event_store = event_store
        self.executor_registry = executor_registry
        self._cache_service = cache_service
        
    async def get_undo_stack(
        self,
        aggregate_type: str,
        aggregate_id: Any,
        depth: int = 10
    ) -> List[UndoableAction]:
        """
        Get list of undoable actions for an aggregate.
        
        Returns actions that:
        - Are not already undone
        - Have a registered UndoExecutor
        """
        events = await self.event_store.get_latest_events(
            aggregate_type, 
            aggregate_id, 
            count=depth * 2  # Fetch more to filter non-undoable
        )
        
        actions = []
        for event in reversed(events):
            # Skip if already undone
            if event.is_undone:
                continue
                
            # Skip if no executor registered
            if not self.executor_registry.has_executor(event.event_type):
                continue
            
            # Hydrate and check can_undo
            domain_event = EventTypeRegistry.hydrate(event)
            if domain_event is None:
                logger.warning(f"Cannot hydrate event type: {event.event_type}")
                continue
            
            executor = self.executor_registry.get(event.event_type)
            try:
                can_undo = await executor.can_undo(domain_event)
            except Exception as e:
                logger.warning(f"Error checking can_undo for {event.event_type}: {e}")
                can_undo = False
            
            if not can_undo:
                continue
                
            actions.append(UndoableAction(
                event_id=event.event_id,
                correlation_id=event.correlation_id,
                event_type=event.event_type,
                description=f"Undo {event.event_type}",
                occurred_at=event.occurred_at,
                user_id=event.user_id,
                can_undo=True,
                causation_id=event.causation_id,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id
            ))
            
            if len(actions) >= depth:
                break
                
        return actions
    
    async def undo(
        self,
        event_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> UndoResult:
        """
        Execute undo operation.
        
        Args:
            event_id: Undo a specific event
            correlation_id: Undo all events with this correlation ID (reverse order)
            user_id: User performing the undo
            
        Returns:
            UndoResult with success status, undone event IDs, and any errors
        """
        if not event_id and not correlation_id:
            return UndoResult(
                success=False, 
                undone_events=[], 
                new_events=[], 
                errors=["Must provide either event_id or correlation_id"]
            )
        
        events_to_undo: List[StoredEvent] = []
        
        # Fetch events to undo
        if event_id:
            stored_event = await self.event_store.get_event(event_id)
            if stored_event:
                events_to_undo = [stored_event]
            else:
                return UndoResult(
                    success=False,
                    undone_events=[],
                    new_events=[],
                    errors=[f"Event not found: {event_id}"]
                )
                
        elif correlation_id:
            stored_events = await self.event_store.get_events_by_correlation(correlation_id)
            # Sort reverse chronological to undo latest first
            events_to_undo = sorted(
                [e for e in stored_events if not e.is_undone],
                key=lambda e: e.id,
                reverse=True
            )
            
        if not events_to_undo:
            return UndoResult(
                success=False,
                undone_events=[],
                new_events=[],
                errors=["No events found to undo"]
            )
        
        # Generate correlation for undo operation
        undo_correlation_id = generate_correlation_id()
        
        undone_ids: List[str] = []
        new_event_ids: List[str] = []
        all_domain_events: List['DomainEventBase'] = []
        errors: List[str] = []
        
        for stored in events_to_undo:
            if stored.is_undone:
                continue
            
            # Check if executor exists
            executor = self.executor_registry.get(stored.event_type)
            if not executor:
                errors.append(f"No undo executor for {stored.event_type}")
                continue
            
            # Hydrate event
            domain_event = EventTypeRegistry.hydrate(stored)
            if domain_event is None:
                errors.append(f"Cannot hydrate event type: {stored.event_type}")
                continue
            
            try:
                # Check if can undo
                can_undo = await executor.can_undo(domain_event)
                if not can_undo:
                    errors.append(f"Cannot undo event {stored.event_id}: business rule violation")
                    continue
                
                # Execute undo - returns compensating events
                compensating_events = await executor.undo(domain_event)
                
                # Persist compensating events
                from .domain_event import enrich_event_metadata
                
                for comp_event in compensating_events:
                    comp_event = enrich_event_metadata(
                        comp_event,
                        correlation_id=undo_correlation_id,
                        causation_id=stored.event_id,
                        user_id=user_id
                    )
                    
                    await self.event_store.append(comp_event)
                    new_event_ids.append(comp_event.event_id)
                    all_domain_events.append(comp_event)
                
                # Mark original as undone
                await self.event_store.mark_as_undone(
                    event_id=stored.event_id,
                    undone_by=user_id or "system",
                    undo_event_id=compensating_events[0].event_id if compensating_events else None
                )
                
                undone_ids.append(stored.event_id)
                
                # Invalidate cache for the affected aggregate
                await self._invalidate_cache(
                    stored.aggregate_type,
                    stored.aggregate_id
                )
                
                logger.info(f"Undone event {stored.event_id} ({stored.event_type})")
                
            except Exception as e:
                errors.append(f"Error undoing {stored.event_id}: {str(e)}")
                logger.exception(f"Error undoing event {stored.event_id}")
        
        return UndoResult(
            success=len(undone_ids) > 0 and len(errors) == 0,
            undone_events=undone_ids,
            new_events=new_event_ids,
            domain_events=all_domain_events,
            correlation_id=undo_correlation_id,
            causation_id=event_id,
            errors=errors
        )
    
    async def _invalidate_cache(
        self,
        aggregate_type: str,
        aggregate_id: Any
    ) -> None:
        """
        Invalidate both domain and query caches for an aggregate.
        
        Cache keys follow the PersistenceDispatcher pattern:
        - EntityName:id (domain cache)
        - EntityName:query:id (query cache)
        """
        if not self._cache_service:
            return
        
        keys = [
            f"{aggregate_type}:{aggregate_id}",
            f"{aggregate_type}:query:{aggregate_id}"
        ]
        
        try:
            await self._cache_service.delete_batch(keys)
            logger.debug(f"Invalidated cache: {keys}")
        except Exception as e:
            logger.warning(f"Cache invalidation failed: {e}")
    
    async def redo(
        self,
        undo_event_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> RedoResult:
        """
        Redo a previously undone action.
        
        Args:
            undo_event_id: The compensating event's ID from the undo operation
            correlation_id: The correlation ID of an undo batch to redo
            user_id: User performing the redo
            
        Returns:
            RedoResult with success status and redone event IDs
        """
        if not undo_event_id and not correlation_id:
            return RedoResult(
                success=False,
                redone_events=[],
                errors=["Must provide either undo_event_id or correlation_id"]
            )
        
        events_to_redo_from: List[StoredEvent] = []
        
        # Get the undo events
        if undo_event_id:
            undo_event = await self.event_store.get_event(undo_event_id)
            if undo_event:
                events_to_redo_from = [undo_event]
            else:
                return RedoResult(
                    success=False,
                    redone_events=[],
                    errors=[f"Undo event not found: {undo_event_id}"]
                )
        elif correlation_id:
            # Get all events with this correlation ID (these are the compensating events)
             events_to_redo_from = await self.event_store.get_events_by_correlation(correlation_id)
             # Sort chronological? Usually redoing a batch should follow the undo order 
             # but since we're re-applying, the order of the undo events themselves 
             # (which were created in a specific sequence during undo) is the key.
             events_to_redo_from = sorted(events_to_redo_from, key=lambda e: e.id)
        
        if not events_to_redo_from:
            return RedoResult(
                success=False,
                redone_events=[],
                errors=["No undo events found to redo"]
            )

        # Generate correlation for redo operation
        redo_correlation_id = generate_correlation_id()
        
        redone_ids: List[str] = []
        redone_domain_events: List['DomainEventBase'] = []
        errors: List[str] = []
        
        for undo_event in events_to_redo_from:
            # The causation_id of the undo event points to the original event
            original_event_id = undo_event.causation_id
            if not original_event_id:
                errors.append(f"Cannot determine original event for undo event {undo_event.event_id}")
                continue
            
            original_event = await self.event_store.get_event(original_event_id)
            if not original_event:
                errors.append(f"Original event {original_event_id} not found")
                continue
            
            if not original_event.is_undone:
                # Might have already been redone
                continue
                
            # Get executor
            executor = self.executor_registry.get(original_event.event_type)
            if not executor:
                errors.append(f"No executor for {original_event.event_type}")
                continue
            
            # Hydrate both events
            original_domain = EventTypeRegistry.hydrate(original_event)
            undo_domain = EventTypeRegistry.hydrate(undo_event)
            
            if not original_domain or not undo_domain:
                errors.append(f"Cannot hydrate events for redo of {original_event_id}")
                continue
            
            try:
                # Execute redo
                redo_events = await executor.redo(original_domain, undo_domain)
                
                from .domain_event import enrich_event_metadata
                
                for redo_event in redo_events:
                    redo_event = enrich_event_metadata(
                        redo_event,
                        correlation_id=redo_correlation_id,
                        user_id=user_id,
                        causation_id=undo_event.event_id
                    )
                    await self.event_store.append(redo_event)
                    redone_ids.append(redo_event.event_id)
                    redone_domain_events.append(redo_event)
                
                # Clear the is_undone flag on the original event
                await self.event_store.mark_as_redone(original_event_id)
                
                # Invalidate cache for the affected aggregate
                await self._invalidate_cache(
                    original_event.aggregate_type,
                    original_event.aggregate_id
                )
                
            except Exception as e:
                errors.append(f"Error during redo of {original_event_id}: {str(e)}")
                logger.exception(f"Error during redo of {original_event_id}")
        
        return RedoResult(
            success=len(redone_ids) > 0 and len(errors) == 0,
            redone_events=redone_ids,
            domain_events=redone_domain_events,
            correlation_id=redo_correlation_id,
            causation_id=undo_event_id or correlation_id,
            errors=errors
        )
