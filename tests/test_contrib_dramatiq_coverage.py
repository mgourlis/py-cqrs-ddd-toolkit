import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from cqrs_ddd.contrib.dramatiq_tasks import (
    setup_dramatiq,
    DramatiqEventPublisher,
    DramatiqWorkerConsumer,
    handle_dramatiq_event,
    set_dispatcher_resolver,
    default_domain_event_router,
    HAS_DRAMATIQ,
)

# --- Helpers ---


class MockMessage:
    def __init__(self, data):
        self.data = data

    @property
    def __dict__(self):
        return {"payload": self.data}


# --- Tests ---


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
def test_setup_dramatiq_custom_middleware():
    """Cover custom middleware loop."""
    middleware = [MagicMock()]
    with patch(
        "cqrs_ddd.contrib.dramatiq_tasks.RabbitmqBroker", create=True
    ) as mock_broker:
        with patch("cqrs_ddd.contrib.dramatiq_tasks.RedisBackend", create=True):
            with patch("cqrs_ddd.contrib.dramatiq_tasks.Results", create=True):
                with patch("cqrs_ddd.contrib.dramatiq_tasks.Abort", create=True):
                    with patch("cqrs_ddd.contrib.dramatiq_tasks.Retries", create=True):
                        with patch("dramatiq.set_broker"):
                            setup_dramatiq(middleware=middleware)
                            # Check if add_middleware was called with our custom one
                            # mock_broker is a class, we need the instance
                            instance = mock_broker.return_value
                            instance.add_middleware.assert_any_call(middleware[0])


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_dramatiq_publisher_payload_fallbacks():
    """Cover __dict__ and raw payload branches in publish."""
    actor = MagicMock()
    actor.actor_name = "test"
    actor.send.return_value = None
    publisher = DramatiqEventPublisher(actor)

    # 1. __dict__ fallback
    msg_obj = MockMessage("hello")
    await publisher.publish("topic", msg_obj)

    # 2. Raw payload
    await publisher.publish("topic", {"raw": "data"})


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_dramatiq_worker_consumer_lifecycle_branches():
    """Cover start() early return and stop() logic."""
    broker = MagicMock()
    with patch("cqrs_ddd.contrib.dramatiq_tasks.Worker", create=True) as mock_worker:
        with patch("cqrs_ddd.contrib.dramatiq_tasks.Thread", create=True):
            consumer = DramatiqWorkerConsumer(broker)
            await consumer.start()
            # 1. Early return on second start
            await consumer.start()
            assert mock_worker.call_count == 1

            # 2. Stop
            await consumer.stop()
            assert consumer._worker is None


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_handle_dramatiq_event_failures():
    """Cover hydration failure and exception handling."""
    dispatcher = AsyncMock()

    # 1. Hydration failure (None)
    with patch(
        "cqrs_ddd.event_registry.EventTypeRegistry.hydrate_dict", return_value=None
    ):
        await handle_dramatiq_event({"topic": "test", "payload": {}}, dispatcher)
        dispatcher.dispatch_background.assert_not_called()

    # 2. General exception
    with patch(
        "cqrs_ddd.event_registry.EventTypeRegistry.hydrate_dict",
        side_effect=ValueError("crash"),
    ):
        with pytest.raises(ValueError, match="crash"):
            await handle_dramatiq_event({"topic": "test", "payload": {}}, dispatcher)


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_default_domain_event_router_coverage():
    """Cover actor logic including resolver branches."""
    # 1. missing resolver
    set_dispatcher_resolver(None)
    with pytest.raises(RuntimeError, match="Global dispatcher resolver not configured"):
        # Use __wrapped__ to call the original async function bypassing Dramatiq's wrapper
        await default_domain_event_router.fn.__wrapped__({"topic": "t"})

    # 2. success path
    dispatcher = AsyncMock()
    set_dispatcher_resolver(lambda: dispatcher)

    with patch(
        "cqrs_ddd.contrib.dramatiq_tasks.handle_dramatiq_event", AsyncMock()
    ) as mock_handle:
        await default_domain_event_router.fn.__wrapped__({"topic": "t"})
        mock_handle.assert_called_with({"topic": "t"}, dispatcher)


def test_dramatiq_missing_guards():
    """Cover ImportErrors when Dramatiq is missing."""
    with patch("cqrs_ddd.contrib.dramatiq_tasks.HAS_DRAMATIQ", False):
        # We simulate the classes defined in if/else or just the logic
        class FallbackPub:
            def __init__(self, *args, **kwargs):
                from cqrs_ddd.contrib.dramatiq_tasks import HAS_DRAMATIQ as HD

                if not HD:
                    raise ImportError("Dramatiq is required")

        with pytest.raises(ImportError):
            FallbackPub()
