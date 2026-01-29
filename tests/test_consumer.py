import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cqrs_ddd.consumer import BaseEventConsumer

@pytest.fixture
def mock_broker():
    broker = MagicMock()
    broker.subscribe = AsyncMock()
    return broker

@pytest.fixture
def mock_dispatcher():
    dispatcher = MagicMock()
    dispatcher.dispatch_background = AsyncMock()
    return dispatcher

@pytest.mark.asyncio
async def test_consumer_lifecycle(mock_broker, mock_dispatcher):
    consumer = BaseEventConsumer(
        broker=mock_broker,
        dispatcher=mock_dispatcher,
        topics=["topic.a"],
        queue_name="q1"
    )
    
    await consumer.start()
    mock_broker.subscribe.assert_awaited_with(
        topic="topic.a", 
        handler=consumer._handle_message, 
        queue_name="q1"
    )
    
    # Start again (noop)
    await consumer.start()
    assert mock_broker.subscribe.call_count == 1
    
    await consumer.stop()
    assert not consumer._running

@pytest.mark.asyncio
async def test_handle_message_success(mock_broker, mock_dispatcher):
    consumer = BaseEventConsumer(mock_broker, mock_dispatcher, ["t"])
    
    # Mock registry hydrate
    with patch("cqrs_ddd.consumer.EventTypeRegistry.hydrate_dict", return_value="hydrated_evt"):
        payload = {"event_type": "MyEvent", "data": "abc"}
        await consumer._handle_message(payload)
        
        mock_dispatcher.dispatch_background.assert_awaited_with("hydrated_evt")

@pytest.mark.asyncio
async def test_handle_message_invalid_payload(mock_broker, mock_dispatcher):
    consumer = BaseEventConsumer(mock_broker, mock_dispatcher, ["t"])
    
    with patch("cqrs_ddd.consumer.logger") as log:
        # Not a dict
        await consumer._handle_message("string")
        log.warning.assert_called_with("Consumer received non-dict payload: <class 'str'>")
        
        # Missing event_type
        await consumer._handle_message({"foo": "bar"})
        log.warning.assert_called_with("Consumer received payload without 'event_type'")

@pytest.mark.asyncio
async def test_handle_message_hydration_fail(mock_broker, mock_dispatcher):
    consumer = BaseEventConsumer(mock_broker, mock_dispatcher, ["t"])
    
    with patch("cqrs_ddd.consumer.EventTypeRegistry.hydrate_dict", return_value=None):
        payload = {"event_type": "Unknown", "data": "abc"}
        with patch("cqrs_ddd.consumer.logger") as log:
            await consumer._handle_message(payload)
            # Should log warning
            assert log.warning.called

@pytest.mark.asyncio
async def test_handle_message_error(mock_broker, mock_dispatcher):
    consumer = BaseEventConsumer(mock_broker, mock_dispatcher, ["t"])
    
    with patch("cqrs_ddd.consumer.EventTypeRegistry.hydrate_dict", side_effect=ValueError("kaboom")):
        payload = {"event_type": "Err", "data": "abc"}
        with patch("cqrs_ddd.consumer.logger") as log:
            await consumer._handle_message(payload)
            log.error.assert_called()
