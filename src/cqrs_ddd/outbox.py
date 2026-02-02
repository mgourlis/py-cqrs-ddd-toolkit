"""Transactional Outbox pattern implementation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import logging
import uuid
import asyncio
from .core import AbstractOutboxMessage
from .protocols import OutboxStorage, MessagePublisher, OutboxProcessor

logger = logging.getLogger("cqrs_ddd")


@dataclass
class OutboxMessage(AbstractOutboxMessage):
    """A message stored in the outbox."""

    id: uuid.UUID
    occurred_at: datetime
    type: str  # e.g., "event"
    topic: str  # Topic/channel name
    payload: Dict[str, Any]
    correlation_id: Optional[str] = None
    status: str = "pending"  # pending, processing, published, failed
    retries: int = 0
    error: Optional[str] = None
    processed_at: Optional[datetime] = None




class InMemoryOutboxStorage:
    """In-memory outbox storage for testing."""

    def __init__(self):
        self._messages: Dict[uuid.UUID, AbstractOutboxMessage] = {}

    async def save(self, message: AbstractOutboxMessage) -> None:
        self._messages[message.id] = message  # type: ignore

    async def get_pending(self, batch_size: int = 50) -> List[AbstractOutboxMessage]:
        pending = [
            m
            for m in self._messages.values()
            if m.status == "pending" or (m.status == "failed" and m.retries < 5)
        ]
        # Sort by age
        pending.sort(key=lambda m: m.occurred_at)
        return pending[:batch_size]

    async def mark_published(self, message_id: uuid.UUID) -> None:
        if message_id in self._messages:
            msg = self._messages[message_id]
            msg.status = "published"
            msg.processed_at = datetime.now(timezone.utc)

    async def mark_failed(self, message_id: uuid.UUID, error: str) -> None:
        if message_id in self._messages:
            msg = self._messages[message_id]
            msg.status = "failed"
            msg.retries += 1
            msg.error = error
            msg.processed_at = datetime.now(timezone.utc)


class OutboxService:
    """
    Core service for processing the transactional outbox.

    This class is logic-only and suitable for use with distributed task
    queues (Dramatiq, Celery, etc.) or simple background workers.
    """

    def __init__(self, storage: OutboxStorage, publisher: MessagePublisher):
        self.storage = storage
        self.publisher = publisher

    async def process_batch(self, batch_size: int = 50) -> int:
        """
        Fetch a batch of pending messages and publish them.

        Returns:
            The number of successfully processed messages.
        """
        messages: List[Any] = await self.storage.get_pending(batch_size)
        if not messages:
            return 0

        count = 0
        for msg in messages:
            try:
                await self.publisher.publish(
                    topic=msg.topic,
                    message=msg.payload,
                    correlation_id=msg.correlation_id,
                )
                await self.storage.mark_published(msg.id)
                count += 1
            except Exception as e:
                logger.error(f"Failed to publish message {msg.id}: {e}")
                await self.storage.mark_failed(msg.id, str(e))

        return count


class OutboxPublisher(MessagePublisher):
    """
    Adapter that allows publishing messages TO the Outbox storage.

    Implements the MessagePublisher protocol, so it can be used interchangeably
    with a real broker, but persists to DB instead of sending immediately.
    """

    def __init__(self, storage: OutboxStorage):
        self.storage = storage

    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """
        Persist message to Outbox storage.
        """
        # Convert message to dict if needed
        if hasattr(message, "to_dict"):
            payload = message.to_dict()
        elif hasattr(message, "__dict__"):
            payload = message.__dict__
        else:
            payload = message

        outbox_msg = OutboxMessage(
            id=uuid.uuid4(),
            occurred_at=datetime.now(timezone.utc),
            type="event",
            topic=topic,
            payload=payload,
            correlation_id=kwargs.get("correlation_id"),
        )

        await self.storage.save(outbox_msg)
        logger.debug(f"Saved message {outbox_msg.id} to outbox")


class OutboxWorker(OutboxProcessor):
    """
    An asyncio-based background worker that processes the outbox in a loop.

    Ensures at-least-once delivery by polling storage at regular intervals.
    """

    def __init__(
        self,
        storage: Optional[OutboxStorage] = None,
        publisher: Optional[MessagePublisher] = None,
        service: Optional[OutboxService] = None,
        poll_interval: float = 5.0,
        batch_size: int = 50,
        run_once: bool = False,
    ):
        if service:
            self.service = service
        elif storage and publisher:
            self.service = OutboxService(storage, publisher)
        else:
            raise ValueError(
                "Either 'service' or both 'storage' and 'publisher' must be provided"
            )

        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.run_once = run_once
        self._running = False
        self._task = None

    async def start(self):
        """
        Start the worker loop in the background.

        It creates an asyncio task that runs the processing loop.
        Idempotent if already running.
        """
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Outbox worker started")

    async def stop(self):
        """
        Stop the worker loop gracefully.

        Sets the running flag to False and waits for the current task to complete.
        """
        self._running = False
        if self._task:
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("Outbox worker stopped")

    async def _run_loop(self):
        """Internal loop that periodically processes messages."""
        while self._running:
            try:
                processed = await self.service.process_batch(self.batch_size)
                if not processed and not self.run_once:
                    await asyncio.sleep(self.poll_interval)

                if self.run_once:
                    break
            except Exception as e:
                logger.error(f"Outbox worker error: {e}", exc_info=True)
                if self.run_once:
                    break
                await asyncio.sleep(self.poll_interval)
