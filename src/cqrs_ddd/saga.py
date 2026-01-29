"""Saga pattern implementation for long-running processes."""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, TypeVar, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import uuid

from .core import Command, CommandResponse
from .ddd import DomainEvent
from .saga_registry import saga_registry

if TYPE_CHECKING:
    from .mediator import Mediator

logger = logging.getLogger("cqrs_ddd")


@dataclass
class SagaContext:
    """State data for a saga instance."""
    saga_id: str
    saga_type: str
    correlation_id: str
    current_step: str
    state: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    error: Optional[str] = None
    is_completed: bool = False
    is_failed: bool = False
    processed_message_ids: List[str] = field(default_factory=list)



class Saga(ABC):
    """
    Base class for Sagas (Process Managers).
    
    A Saga coordinates long-running flows by listening to events and 
    issuing commands. It maintains state across multiple transactions.
    
    Usage:
        class OrderSaga(Saga):
            @saga_step(OrderCreated)
            async def on_order_created(self, event: OrderCreated):
                self.context.state['order_id'] = event.order_id
                await self.dispatch(ProcessPayment(amount=event.amount))
            
            @saga_step(PaymentProcessed)
            async def on_payment_processed(self, event: PaymentProcessed):
                await self.dispatch(ShipOrder(order_id=self.context.state['order_id']))
                self.complete()
    """
    
    _handlers: Dict[Type, str] = {}


    def __init_subclass__(cls, **kwargs):
        """Build handler map from decorated methods."""
        super().__init_subclass__(**kwargs)
        
        # Inherit handlers from base classes
        cls._handlers = {}
        for base in reversed(cls.__mro__):
            if hasattr(base, "_handlers"):
                cls._handlers.update(base._handlers)
        
        # Discover new handlers in current class
        found_new = False
        for name, attr in cls.__dict__.items():
            if hasattr(attr, "_saga_event_type"):
                event_type = attr._saga_event_type
                cls._handlers[event_type] = name
                found_new = True
                
        # Auto-register with global registry ONLY if it's a concrete class
        # and has handlers (either new or inherited)
        import inspect
        if not inspect.isabstract(cls) and cls._handlers:
            for event_type in cls._handlers:
                saga_registry.register(event_type, cls)

    def __init__(self, context: SagaContext, mediator: "Mediator"):
        self.context = context
        self.mediator = mediator
        self._commands_to_dispatch: List[Command] = []
    
    @property
    def id(self) -> str:
        return self.context.saga_id
    
    @property
    def is_active(self) -> bool:
        return not (self.context.is_completed or self.context.is_failed)
    
    async def handle_event(self, event: DomainEvent) -> None:
        """Handle an incoming domain event."""
        event_type = type(event)
        handler_name = self._handlers.get(event_type)
        
        if handler_name and hasattr(self, handler_name):
            handler = getattr(self, handler_name)
            
            # Update context history
            self.context.history.append({
                "action": "handle_event",
                "event_type": event_type.__name__,
                "occurred_at": datetime.now(timezone.utc).isoformat()
            })
            self.context.updated_at = datetime.now(timezone.utc)
            
            try:
                try:
                    await handler(event)
                except Exception as e:
                    self.fail(str(e))
                    logger.error(f"Saga {self.id} failed processing {event_type.__name__}: {e}")
                    # Production enhancement: trigger compensation if failed
                    try:
                        await self.compensate()
                    except Exception as comp_err:
                        logger.critical(f"Compensation failed for Saga {self.id}: {comp_err}")
                    raise e
            finally:
                # Dispatch accumulated commands (including any from compensation)
                await self._dispatch_commands()
    
    def dispatch_command(self, command: Command) -> None:
        """Queue a command to be dispatched."""
        self._commands_to_dispatch.append(command)
    
    def complete(self) -> None:
        """Mark saga as successfully completed."""
        self.context.is_completed = True
        self.context.completed_at = datetime.now(timezone.utc)
        self.context.updated_at = datetime.now(timezone.utc)
    
    def fail(self, error: str) -> None:
        """Mark saga as failed."""
        self.context.is_failed = True
        self.context.failed_at = datetime.now(timezone.utc)
        self.context.error = error
        self.context.updated_at = datetime.now(timezone.utc)

    @abstractmethod
    async def compensate(self) -> None:
        """
        Execute compensating actions to undo the effects of this saga.
        To be implemented by subclasses.
        """
        ...

    
    async def _dispatch_commands(self) -> None:
        """Internal: dispatch all queued commands."""
        for command in self._commands_to_dispatch:
            # Propagate correlation ID if possible
            if hasattr(command, 'correlation_id') and not command.correlation_id:
                command.correlation_id = self.context.correlation_id
            
            try:
                await self.mediator.send(command)
                
                self.context.history.append({
                    "action": "dispatch_command",
                    "command_type": type(command).__name__,
                    "occurred_at": datetime.now(timezone.utc).isoformat()
                })
            except Exception as e:
                logger.error(f"Saga {self.id} failed dispatching command: {e}")
                self.fail(str(e))
                await self.compensate()
                # Re-raise to let SagaManager handle it (e.g. still save the failed state)
                raise
        
        self._commands_to_dispatch.clear()
        self.context.updated_at = datetime.now(timezone.utc)


def saga_step(event_type: Type):
    """
    Decorator to mark a method as a saga step handler.
    
    Ideally this would register the handler. For the simple implementation
    above using _get_handler_name convention, this is just a marker.
    A more advanced implementation would use a registry.
    """
    def decorator(func):
        func._saga_event_type = event_type
        return func
    return decorator


# Protocols imported from .protocols to resolve circular deps
from .protocols import SagaRepository


class InMemorySagaRepository:
    """In-memory saga repository for testing."""
    
    def __init__(self):
        self._sagas: Dict[str, SagaContext] = {}
        # (saga_type, correlation_id) -> saga_id
        self._correlation_type_map: Dict[tuple, str] = {}
    
    async def save(self, context: SagaContext) -> None:
        self._sagas[context.saga_id] = context
        key = (context.saga_type, context.correlation_id)
        self._correlation_type_map[key] = context.saga_id
    
    async def load(self, saga_id: str) -> Optional[SagaContext]:
        return self._sagas.get(saga_id)
    
    async def find_by_correlation_id(self, correlation_id: str, saga_type: Optional[str] = None) -> Optional[SagaContext]:
        if not saga_type:
            # Fallback for legacy or unknown types (though not ideal)
            # Find any saga with this correlation_id
            for (st, cid), sid in self._correlation_type_map.items():
                if cid == correlation_id:
                    return self._sagas.get(sid)
            return None
            
        key = (saga_type, correlation_id)
        saga_id = self._correlation_type_map.get(key)
        if saga_id:
            return self._sagas.get(saga_id)
        return None


class SagaManager:
    """
    Orchestrates saga execution.
    
    Listens for events, loads corresponding saga (or creating new),
    and executes steps.
    """
    
    def __init__(
        self, 
        repository: SagaRepository,
        mediator: "Mediator",
        saga_registry: Optional[Dict[Type[DomainEvent], List[Type['Saga']]]] = None,
        lock_strategy: Optional['LockStrategy'] = None
    ):
        self.repository = repository
        self.mediator = mediator
        self.saga_registry = saga_registry  # Optional override
        self.lock_strategy = lock_strategy

    
    async def handle_event(self, event: DomainEvent) -> None:
        """
        Process an event against registered sagas.
        """
        event_type = type(event)
        
        # Determine which sagas to run
        if self.saga_registry is not None:
            # Use explicit registry if provided (legacy/override)
            saga_classes = self.saga_registry.get(event_type, [])
            if not isinstance(saga_classes, list):
                saga_classes = [saga_classes]
        else:
            # Use global registry
            saga_classes = saga_registry.get_sagas_for_event(event_type)
        
        if not saga_classes:
            return
        
        correlation_id = getattr(event, 'correlation_id', None)
        if not correlation_id:
            # Try to extract from metadata or other fields if possible
            if hasattr(event, 'metadata') and 'correlation_id' in event.metadata:
                correlation_id = event.metadata['correlation_id']
            else:
                logger.warning(f"Event {event_type.__name__} has no correlation_id, cannot match saga")
                return
        
        for saga_class in saga_classes:
            await self._process_saga(saga_class, event, correlation_id)

    async def _process_saga(self, saga_class: Type['Saga'], event: DomainEvent, correlation_id: str) -> None:
        """Execute a single saga instance."""
        try:
            # Use lock to prevent concurrent processing of the same saga instance
            lock_token = None
            if self.lock_strategy:
                # We lock by saga class AND correlation_id to allow different sagas 
                # for the same event to run in parallel
                lock_key = f"saga:{saga_class.__name__}:{correlation_id}"
                lock_token = await self.lock_strategy.acquire("saga", lock_key)

            try:
                # Load context by (type, correlation)
                saga_type_name = saga_class.__name__
                context = await self.repository.find_by_correlation_id(
                    correlation_id, 
                    saga_type=saga_type_name
                )
                
                if not context:
                    context = SagaContext(
                        saga_id=str(uuid.uuid4()),
                        saga_type=saga_type_name,
                        correlation_id=correlation_id,
                        current_step="start"
                    )
                    # Save immediately so recursive calls can find this context
                    await self.repository.save(context)
                
                if context.is_completed or context.is_failed:
                    return

                # Idempotency check
                event_id = getattr(event, 'event_id', getattr(event, 'id', None))
                if event_id and event_id in context.processed_message_ids:
                    logger.info(f"Saga {context.saga_id} already processed event {event_id}")
                    return

                try:
                    # Instantiate and run saga
                    saga = saga_class(context, self.mediator)
                    await saga.handle_event(event)

                    # Update processed messages
                    if event_id:
                        context.processed_message_ids.append(event_id)
                except Exception:
                    # Ensure we save the failed state before propagating
                    await self.repository.save(context)
                    raise

                # Save state
                await self.repository.save(context)

            finally:
                if self.lock_strategy and lock_token:
                    lock_key = f"saga:{saga_class.__name__}:{correlation_id}"
                    await self.lock_strategy.release("saga", lock_key, lock_token)
                    
        except Exception as e:
            logger.error(f"Error processing Saga {saga_class.__name__}: {e}")
            raise

