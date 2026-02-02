import pytest
from unittest.mock import MagicMock, patch, AsyncMock

try:
    from cqrs_ddd.contrib.dramatiq_tasks import (
        setup_dramatiq,
        DramatiqEventPublisher,
        DramatiqWorkerConsumer,
        handle_dramatiq_event,
    )
    # import dramatiq
    HAS_DRAMATIQ = True
except (ImportError, Exception):
    HAS_DRAMATIQ = False


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
def test_setup_dramatiq():
    with patch("cqrs_ddd.contrib.dramatiq_tasks.HAS_DRAMATIQ", True):
        # Mock RabbitmqBroker and RedisBackend
        with patch(
            "cqrs_ddd.contrib.dramatiq_tasks.RabbitmqBroker", create=True
        ) as mock_broker:
            with patch(
                "cqrs_ddd.contrib.dramatiq_tasks.RedisBackend", create=True
            ) as _:
                with patch("cqrs_ddd.contrib.dramatiq_tasks.Results", create=True):
                    with patch("cqrs_ddd.contrib.dramatiq_tasks.Abort", create=True):
                        with patch(
                            "cqrs_ddd.contrib.dramatiq_tasks.Retries", create=True
                        ):
                            with patch(
                                "cqrs_ddd.contrib.dramatiq_tasks.AsyncIO", create=True
                            ):
                                with patch(
                                    "dramatiq.set_broker", create=True
                                ) as mock_set:
                                    broker = setup_dramatiq(
                                        broker_url="amqp://test",
                                        redis_url="redis://test",
                                    )
                                    assert broker
                                    mock_set.assert_called_once()
                                    mock_broker.assert_called_once_with(
                                        url="amqp://test"
                                    )


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_dramatiq_publisher():
    with patch("cqrs_ddd.contrib.dramatiq_tasks.HAS_DRAMATIQ", True):
        actor = MagicMock()
        actor.actor_name = "test_actor"
        publisher = DramatiqEventPublisher(actor)

        msg = MagicMock()
        msg.to_dict.return_value = {"msg": "hello"}

        # We mock loop.run_in_executor
        with patch("asyncio.get_running_loop") as mock_loop:
            mock_loop.return_value.run_in_executor = AsyncMock()
            await publisher.publish("topic", msg, corr_id="123")

            # Verify executor call
            mock_loop.return_value.run_in_executor.assert_called()


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_dramatiq_worker_consumer():
    with patch("cqrs_ddd.contrib.dramatiq_tasks.HAS_DRAMATIQ", True):
        broker = MagicMock()
        # Ensure Worker can be found via patch
        with patch(
            "cqrs_ddd.contrib.dramatiq_tasks.Worker", create=True
        ) as mock_worker:
            with patch("cqrs_ddd.contrib.dramatiq_tasks.Thread") as mock_thread:
                consumer = DramatiqWorkerConsumer(broker, queues=["test"])
                await consumer.start()
                assert mock_worker.called
                assert mock_thread.called

                await consumer.stop()
                mock_worker.return_value.stop.assert_called()


@pytest.mark.skipif(not HAS_DRAMATIQ, reason="Dramatiq not installed")
@pytest.mark.asyncio
async def test_handle_dramatiq_event_logic():
    dispatcher = AsyncMock()
    message = {"topic": "UserCreated", "payload": {"id": "1"}}

    # Mock EventTypeRegistry.hydrate_dict
    with patch(
        "cqrs_ddd.event_registry.EventTypeRegistry.hydrate_dict"
    ) as mock_hydrate:
        mock_hydrate.return_value = MagicMock()
        await handle_dramatiq_event(message, dispatcher)
        dispatcher.dispatch_background.assert_awaited()
