import pytest
import asyncio
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from cqrs_ddd.outbox import (
    OutboxMessage,
    InMemoryOutboxStorage,
    OutboxService,
    OutboxPublisher,
    OutboxWorker,
)
from cqrs_ddd.protocols import MessagePublisher

# --- Fixtures ---


class MockPublisher(MessagePublisher):
    def __init__(self):
        self.published = []

    async def publish(self, topic: str, message: Any, **kwargs):
        self.published.append((topic, message, kwargs))


@pytest.fixture
def storage():
    return InMemoryOutboxStorage()


@pytest.fixture
def publisher():
    return MockPublisher()


# --- Tests ---


@pytest.mark.asyncio
async def test_in_memory_storage(storage):
    msg = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=MagicMock(),
        type="event",
        topic="test",
        payload={"a": 1},
    )

    await storage.save(msg)

    pending = await storage.get_pending()
    assert len(pending) == 1
    assert pending[0].id == msg.id

    await storage.mark_published(msg.id)
    pending_after = await storage.get_pending()
    assert len(pending_after) == 0
    assert msg.status == "published"


@pytest.mark.asyncio
async def test_outbox_service_flow(storage, publisher):
    service = OutboxService(storage, publisher)

    # 1. Create message
    msg = OutboxMessage(
        id=uuid.uuid4(),
        occurred_at=MagicMock(),
        type="event",
        topic="test",
        payload={"key": "val"},
    )
    await storage.save(msg)

    # 2. Process
    count = await service.process_batch(10)
    assert count == 1

    # 3. Verify
    assert len(publisher.published) == 1
    assert publisher.published[0][0] == "test"
    assert msg.status == "published"


@pytest.mark.asyncio
async def test_outbox_service_failure(storage, publisher):
    # Mock publisher failure
    publisher.publish = AsyncMock(side_effect=Exception("Broker down"))
    service = OutboxService(storage, publisher)

    msg = OutboxMessage(
        id=uuid.uuid4(), occurred_at=MagicMock(), type="event", topic="fail", payload={}
    )
    await storage.save(msg)

    # Process
    with patch("cqrs_ddd.outbox.logger"):
        await service.process_batch()

    assert msg.status == "failed"
    assert msg.retries == 1
    assert "Broker down" in msg.error


@pytest.mark.asyncio
async def test_outbox_publisher_adapter(storage):
    adapter = OutboxPublisher(storage)

    await adapter.publish("topic-1", {"p": 1}, correlation_id="c1")

    pending = await storage.get_pending()
    assert len(pending) == 1
    msg = pending[0]
    assert msg.topic == "topic-1"
    assert msg.payload == {"p": 1}
    assert msg.correlation_id == "c1"


@pytest.mark.asyncio
async def test_outbox_worker_lifecycle(storage, publisher):
    service = OutboxService(storage, publisher)
    worker = OutboxWorker(service=service, poll_interval=0.01)
    # Don't use run_once=True to test full start/stop logic if possible,
    # but run_once=False requires careful stop.
    # Let's use run_once=False but stop quickly.

    # Add message
    msg = OutboxMessage(uuid.uuid4(), MagicMock(), "e", "t", {})
    await storage.save(msg)

    # Start
    await worker.start()

    # Poll for completion inside test, safe timeout
    for _ in range(50):
        if msg.status == "published":
            break
        await asyncio.sleep(0.01)

    await worker.stop()

    # Task should be done
    assert msg.status == "published"


@pytest.mark.asyncio
async def test_outbox_worker_init_validation():
    with pytest.raises(ValueError):
        OutboxWorker(storage=None, publisher=None, service=None)


@pytest.mark.asyncio
async def test_outbox_worker_error_handling_safe(storage, publisher):
    # Mock service to raise once then succeed or just raise
    mock_service = MagicMock()
    mock_service.process_batch.side_effect = [Exception("Fatal"), 0]

    # Use run_once=True to ensure it exits
    worker = OutboxWorker(service=mock_service, poll_interval=0.001, run_once=True)

    with patch("cqrs_ddd.outbox.logger") as mock_log:
        await worker.start()

        # Wait for task completion
        if worker._task:
            await asyncio.wait_for(worker._task, timeout=1.0)

        # Should have logged error
        mock_log.error.assert_called()
