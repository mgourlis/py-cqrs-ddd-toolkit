"""
Dramatiq Integration for Task Management.

This module provides helpers to configure Dramatiq with RabbitMQ and Redis,
enabling robust task management with abort capabilities and AsyncIO support.
"""
from typing import Optional, List, Any, Callable, Dict
import logging
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

try:
    import dramatiq
    from dramatiq.brokers.rabbitmq import RabbitmqBroker
    from dramatiq.results import Results
    from dramatiq.results.backends import RedisBackend
    from dramatiq.middleware import AsyncIO, Retries
    from dramatiq_abort import Abort
    from dramatiq import Worker
    HAS_DRAMATIQ = True
except ImportError:
    HAS_DRAMATIQ = False

from ..protocols import MessagePublisher, EventConsumer
from ..event_registry import EventTypeRegistry

logger = logging.getLogger("cqrs_ddd.contrib.dramatiq")


def setup_dramatiq(
    broker_url: str = "amqp://guest:guest@localhost:5672",
    redis_url: str = "redis://localhost:6379/0",
    enable_asyncio: bool = True,
    middleware: List[Any] = None
) -> "dramatiq.Broker":
    """
    Configure Dramatiq with RabbitMQ broker and Redis backend.
    
    Includes standard middleware stack:
    1. Results (Redis)
    2. Abort (Redis)
    3. Retries (default: 3)
    4. AsyncIO (optional, default: True)
    
    Args:
        broker_url: Connection string for RabbitMQ.
        redis_url: Connection string for Redis (used for results/aborting).
        enable_asyncio: Add AsyncIO middleware for async actors.
        middleware: Optional list of additional middleware to add.
        
    Returns:
        The configured Dramatiq broker instance.
    """
    if not HAS_DRAMATIQ:
        raise ImportError(
            "Dramatiq or dependencies are missing. "
            "Install with: pip install 'py-cqrs-ddd-toolkit[dramatiq,dramatiq-abort]'"
        )

    # 1. Configure Result Backend (Redis)
    backend = RedisBackend(url=redis_url)

    # 2. Configure Broker (RabbitMQ)
    broker = RabbitmqBroker(url=broker_url)
    
    # 3. Add Core Middleware
    broker.add_middleware(Results(backend=backend))
    broker.add_middleware(Abort(backend=backend))
    broker.add_middleware(Retries(max_retries=3))
    
    if enable_asyncio:
        broker.add_middleware(AsyncIO())
    
    # 4. Add Custom Middleware
    if middleware:
        for m in middleware:
            broker.add_middleware(m)

    # 5. Set Global Broker
    dramatiq.set_broker(broker)
    
    logger.info("Dramatiq configured with RabbitMQ, Redis, Abort, and AsyncIO support")
    return broker


class DramatiqEventPublisher(MessagePublisher):
    """
    MessagePublisher implementation that sends events to a Dramatiq Actor.
    
    This bridges the Event Bus pattern onto the Task Queue infrastructure.
    Events are published by enqueueing a job for the specified 'routing_actor'.
    """
    
    def __init__(self, routing_actor: Any):
        """
        Args:
            routing_actor: A Dramatiq actor (function decorated with @actor)
                           that accepts the event payload.
        """
        if not HAS_DRAMATIQ:
             raise ImportError("Dramatiq is required for DramatiqEventPublisher")
        self.routing_actor = routing_actor
        
    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """
        Publish a message by sending it to the routing actor.
        
        The 'topic' is included in the message metadata wrapper if possible,
        or just handled by the actor based on payload content.
        """
        # Dramatiq actors process things nicely if passed as JSON-compatible dicts.
        if hasattr(message, "to_dict"):
            payload = message.to_dict()
        elif hasattr(message, "__dict__"):
            payload = message.__dict__
        else:
            payload = message

        # We wrap it to preserve the 'topic' concept if the actor needs it
        wrapped_message = {
            "topic": topic,
            "payload": payload,
            "metadata": kwargs
        }
        
        # Send to Dramatiq
        # .send() is synchronous, so we run it in an executor to avoid blocking the event loop
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, 
            lambda: self.routing_actor.send(wrapped_message)
        )
        logger.debug(f"Published event '{topic}' to Dramatiq actor {self.routing_actor.actor_name}")



class DramatiqWorkerConsumer(EventConsumer):
    """
    EventConsumer implementation that runs a Dramatiq Worker in a background thread.
    
    This fulfills the EventConsumer protocol ('start'/'stop') by managing
    the lifecycle of a local Dramatiq worker.
    
    USAGE RECOMMENDATION:
    - **Development/Testing**: Excellent for running API + Worker in a single process.
    - **Low-Traffic**: Good for single-container deployments.
    - **Production**: Consider using the standard `dramatiq` CLI for process management,
      reloading, and better signal handling, instead of this embedded consumer.
    """
    
    def __init__(self, broker: "dramatiq.Broker", queues: List[str] = None, worker_threads: int = 1):
        """
        Args:
            broker: Configured Dramatiq broker
            queues: List of queue names to listen to. Defaults to ['default'].
            worker_threads: Number of worker threads per process.
        """
        if not HAS_DRAMATIQ:
            raise ImportError("Dramatiq is required for DramatiqWorkerConsumer")
            
        self.broker = broker
        self.queues = queues or ["default"]
        self.worker_threads = worker_threads
        self._worker: Optional[Worker] = None
        self._thread: Optional[Thread] = None
        
    async def start(self) -> None:
        """Start the Dramatiq worker in a background thread."""
        if self._worker:
            return

        logger.info(f"Starting DramatiqWorkerConsumer for queues: {self.queues}")
        
        self._worker = Worker(self.broker, queues=self.queues, worker_threads=self.worker_threads)
        
        # Worker.start() blocks, so run in a thread
        self._thread = Thread(target=self._worker.start, daemon=True)
        self._thread.start()
        
        logger.info("DramatiqWorkerConsumer started")

    async def stop(self) -> None:
        """Stop the Dramatiq worker."""
        if self._worker:
            logger.info("Stopping DramatiqWorkerConsumer...")
            self._worker.stop()
            if self._thread:
                self._thread.join(timeout=5.0)
            self._worker = None
            self._thread = None
            logger.info("DramatiqWorkerConsumer stopped")



# Global Dispatcher Resolver
# Since Dramatiq actors are global functions, they need a way to access the 
# EventDispatcher instance from the application container without circular imports.
_DISPATCHER_RESOLVER: Optional[Callable[[], Any]] = None

def set_dispatcher_resolver(resolver: Callable[[], Any]) -> None:
    """
    Set the function used to resolve the EventDispatcher instance.
    
    Usage:
        set_dispatcher_resolver(lambda: container.event_dispatcher())
    """
    global _DISPATCHER_RESOLVER
    _DISPATCHER_RESOLVER = resolver


async def handle_dramatiq_event(
    message: Dict[str, Any], 
    dispatcher: Any
) -> None:
    """
    Helper to process an event received via Dramatiq (Async).
    
    Deserializes the payload (if it matches a known Domain Event) and 
    dispatches it to the local EventDispatcher.
    """
    topic = message.get("topic")
    payload = message.get("payload", {})
    
    logger.debug(f"Received Dramatiq event user task: {topic}")
    
    async def _dispatch():
        # Hydrate the event using the registry
        event = EventTypeRegistry.hydrate_dict(topic, payload)
        
        if not event:
            logger.warning(
                f"Could not hydrate event '{topic}'. "
                "Ensure the event class is registered with EventTypeRegistry."
            )
            return

        logger.info(f"Dispatching hydrated event {topic}...")
        await dispatcher.dispatch_background(event)
        
    try:
        # Run the hydration and dispatch logic
        await _dispatch()
        
    except Exception as e:
        logger.error(f"Failed to dispatch Dramatiq event: {e}", exc_info=True)
        raise


if HAS_DRAMATIQ:
    @dramatiq.actor(queue_name="cqrs_ddd_dramatiq_events")
    async def default_domain_event_router(message: Dict[str, Any]) -> None:
        """
        Default Async Dramatiq Actor for routing Domain Events.
        
        Requires `set_dispatcher_resolver` to be configured at startup.
        """
        if not _DISPATCHER_RESOLVER:
            raise RuntimeError(
                "Global dispatcher resolver not configured. "
                "Call `set_dispatcher_resolver(lambda: container.event_dispatcher())` at startup."
            )
        
        dispatcher = _DISPATCHER_RESOLVER()
        await handle_dramatiq_event(message, dispatcher)
