import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from cqrs_ddd.publishers import (
    PublishingEventHandler,
    TopicRoutingPublisher,
    route_to,
    auto_register_routed_events,
    EVENT_DESTINATIONS,
    ROUTED_EVENT_CLASSES,
)

# --- Fixtures ---


@pytest.fixture(autouse=True)
def clean_registry():
    # Backup
    old_dest = EVENT_DESTINATIONS.copy()
    old_classes = list(ROUTED_EVENT_CLASSES)

    EVENT_DESTINATIONS.clear()
    ROUTED_EVENT_CLASSES.clear()

    yield

    # Restore
    EVENT_DESTINATIONS.clear()
    EVENT_DESTINATIONS.update(old_dest)

    ROUTED_EVENT_CLASSES.clear()
    ROUTED_EVENT_CLASSES.extend(old_classes)


# --- Tests ---


@pytest.mark.asyncio
async def test_publishing_event_handler():
    mock_pub = MagicMock()
    mock_pub.publish = AsyncMock()

    handler = PublishingEventHandler(mock_pub)

    class MyEvent:
        pass

    event = MyEvent()

    await handler.handle(event)

    mock_pub.publish.assert_awaited_with("MyEvent", event)


@pytest.mark.asyncio
async def test_publishing_event_handler_error():
    mock_pub = MagicMock()
    mock_pub.publish = AsyncMock(side_effect=ValueError("fail"))

    handler = PublishingEventHandler(mock_pub)

    with pytest.raises(ValueError):
        with patch("cqrs_ddd.publishers.logger"):
            await handler.handle("event")


def test_route_to_decorator():
    @route_to("dramatiq")
    class RoutedEvent:
        pass

    assert EVENT_DESTINATIONS["RoutedEvent"] == "dramatiq"
    assert RoutedEvent in ROUTED_EVENT_CLASSES


def test_auto_register():
    @route_to("dest1")
    class Ev1:
        pass

    @route_to("dest2")
    class Ev2:
        pass

    mock_dispatcher = MagicMock()
    handler = "my_handler"

    count = auto_register_routed_events(mock_dispatcher, handler)
    assert count == 2
    assert mock_dispatcher.register.call_count == 2


@pytest.mark.asyncio
async def test_topic_routing_publisher_logic():
    dest_pub = MagicMock()
    dest_pub.publish = AsyncMock()

    route_pub = MagicMock()
    route_pub.publish = AsyncMock()

    default_pub = MagicMock()
    default_pub.publish = AsyncMock()

    publisher = TopicRoutingPublisher(
        routes={}, destinations={"my_dest": dest_pub}, default=default_pub
    )

    # Test Manual Register
    publisher.register("specific_topic", route_pub)

    # 1. Direct route
    await publisher.publish("specific_topic", "msg1")
    route_pub.publish.assert_awaited_with("specific_topic", "msg1")

    # 2. Destination route (mock global registry)
    EVENT_DESTINATIONS["AutoTopic"] = "my_dest"
    await publisher.publish("AutoTopic", "msg2")
    dest_pub.publish.assert_awaited_with("AutoTopic", "msg2")

    # 3. Default
    await publisher.publish("unknown", "msg3")
    default_pub.publish.assert_awaited_with("unknown", "msg3")


@pytest.mark.asyncio
async def test_topic_routing_no_publisher():
    publisher = TopicRoutingPublisher(default=None)
    with patch("cqrs_ddd.publishers.logger") as mock_log:
        await publisher.publish("lost", "msg")
        mock_log.warning.assert_called()
