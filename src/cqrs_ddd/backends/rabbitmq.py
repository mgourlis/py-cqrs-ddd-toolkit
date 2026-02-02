"""RabbitMQ backend for Message Broker."""

import json
import logging
from typing import Callable, Any, Optional

try:
    import aio_pika

    HAS_AIO_PIKA = True
except ImportError:
    HAS_AIO_PIKA = False
    aio_pika = Any

from ..protocols import MessageBroker

logger = logging.getLogger("cqrs_ddd")


class RabbitMQBroker(MessageBroker):
    """
    RabbitMQ implementation of MessageBroker using aio_pika.

    Publishes events to an exchange (default: "domain_events").
    Subscribers bind queues to this exchange with routing keys (topics).
    """

    def __init__(
        self,
        url: str = "amqp://guest:guest@localhost/",
        exchange_name: str = "domain_events",
    ):
        if not HAS_AIO_PIKA:
            raise ImportError(
                "aio_pika is required. Install with: pip install aio_pika"
            )

        self.url = url
        self.exchange_name = exchange_name
        self._connection: Optional[aio_pika.Connection] = None
        self._channel: Optional[aio_pika.Channel] = None
        self._exchange: Optional[aio_pika.Exchange] = None
        self._closing = False

    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        if self._connection and not self._connection.is_closed:
            return

        logger.info(f"Connecting to RabbitMQ at {self.url}")
        self._connection = await aio_pika.connect_robust(self.url)
        self._channel = await self._connection.channel()

        # Declare the exchange (Topic exchange allows versatile routing)
        self._exchange = await self._channel.declare_exchange(
            self.exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
        )
        logger.info(f"Connected to RabbitMQ, exchange: {self.exchange_name}")

    async def close(self) -> None:
        """Close connection."""
        self._closing = True
        if self._connection:
            await self._connection.close()
            logger.info("Closed RabbitMQ connection")

    async def publish(self, topic: str, message: Any, **kwargs) -> None:
        """
        Publish a message to the exchange.

        Args:
            topic: Routing key (e.g. "product.created")
            message: OutboxMessage or dict
            **kwargs: Metadata to include in headers
        """
        if not self._exchange:
            await self.connect()

        # Serialize message
        if hasattr(message, "to_dict"):
            body = json.dumps(message.to_dict()).encode()
        elif hasattr(message, "__dict__"):
            body = json.dumps(message.__dict__).encode()
        else:
            body = json.dumps(message).encode()

        # Merge kwargs into headers
        headers = {"correlation_id": getattr(message, "correlation_id", None)}
        headers.update(kwargs)

        # Publish
        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                headers=headers,
            ),
            routing_key=topic,
        )
        logger.debug(f"Published message to {topic}")

    async def subscribe(
        self, topic: str, handler: Callable, queue_name: Optional[str] = None
    ) -> None:
        """
        Subscribe to a topic.

        Creates a durable queue bound to the exchange with the given topic (routing key).

        Args:
            topic: Routing key to subscribe to (e.g. "product.#")
            handler: Async function to handle decoded messages
            queue_name: Optional explicit queue name. If not provided,
                        it will be generated as "queue_<topic>".
        """
        if not self._channel:
            await self.connect()

        # Determine queue name
        actual_queue_name = queue_name or f"queue_{topic.replace('.', '_')}"

        queue = await self._channel.declare_queue(actual_queue_name, durable=True)

        await queue.bind(self._exchange, routing_key=topic)

        async def on_message(message: aio_pika.IncomingMessage):
            async with message.process():
                try:
                    payload = json.loads(message.body.decode())
                    await handler(payload)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # In a real app, you might want dead-lettering here

        await queue.consume(on_message)
        logger.info(f"Subscribed to {topic} with queue {queue_name}")
