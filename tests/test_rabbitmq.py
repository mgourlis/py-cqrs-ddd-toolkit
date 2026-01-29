import json
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from cqrs_ddd.backends import rabbitmq as rm_mod
from cqrs_ddd.outbox import OutboxMessage


@pytest.mark.asyncio
async def test_constructor_requires_aio_pika(monkeypatch):
    monkeypatch.setattr(rm_mod, 'HAS_AIO_PIKA', False)
    with pytest.raises(ImportError):
        rm_mod.RabbitMQBroker()
    # restore
    monkeypatch.setattr(rm_mod, 'HAS_AIO_PIKA', True)


@pytest.mark.asyncio
async def test_publish_serializes_and_publishes(monkeypatch):
    # Prepare fake aio_pika module
    class FakeMessage:
        def __init__(self, body, delivery_mode=None, content_type=None, headers=None):
            self.body = body
            self.delivery_mode = delivery_mode
            self.content_type = content_type
            self.headers = headers

    FakeDelivery = SimpleNamespace(PERSISTENT=2)
    FakeExchangeType = SimpleNamespace(TOPIC='topic')

    # Mocks for connection/channel/exchange (implemented as real async functions to avoid AsyncMock warnings)
    publish_calls = []
    async def publish(msg, routing_key):
        publish_calls.append((msg, routing_key))
    exchange_mock = SimpleNamespace(publish=publish)

    async def declare_exchange(name, ex_type, durable=True):
        return exchange_mock
    channel_mock = SimpleNamespace(declare_exchange=declare_exchange)

    async def channel():
        return channel_mock
    connection_mock = SimpleNamespace(channel=channel)

    async def connect_robust(url):
        return connection_mock

    fake_aio = SimpleNamespace(
        connect_robust=connect_robust,
        Message=FakeMessage,
        DeliveryMode=FakeDelivery,
        ExchangeType=FakeExchangeType
    )

    monkeypatch.setattr(rm_mod, 'aio_pika', fake_aio)
    monkeypatch.setattr(rm_mod, 'HAS_AIO_PIKA', True)

    broker = rm_mod.RabbitMQBroker(url='amqp://test/', exchange_name='ex')

    # Publish a simple dict
    await broker.publish('topic.key', {'a': 1}, meta='X')
    assert publish_calls, "exchange.publish not called"
    msg, rk = publish_calls[-1]
    assert rk == 'topic.key'
    assert json.loads(msg.body.decode()) == {'a': 1}
    assert msg.content_type == 'application/json'
    assert msg.headers.get('meta') == 'X'

    # Publish an OutboxMessage
    publish_calls.clear()
    out = OutboxMessage(id='1', occurred_at=None, type='event', topic='t', payload={'x':1}, correlation_id='corr')
    await broker.publish('t.key', out)
    msg, rk = publish_calls[-1]
    assert json.loads(msg.body.decode())['payload'] == {'x':1}
    assert msg.headers.get('correlation_id') == 'corr'


@pytest.mark.asyncio
async def test_subscribe_declares_queue_and_consumes(monkeypatch, caplog):
    caplog.set_level('INFO')

    # Prepare fake aio_pika module and mocks (avoid AsyncMock internals)
    queue_calls = {'called': False, 'callbacks': []}

    async def queue_consume(cb):
        queue_calls['called'] = True
        queue_calls['callbacks'].append(cb)

    async def bind(exchange, routing_key):
        queue_calls.setdefault('binds', []).append((exchange, routing_key))

    async def fake_declare_queue(name, durable=True):
        q = SimpleNamespace(bind=bind, consume=queue_consume)
        return q

    exchange_mock = SimpleNamespace()
    async def declare_queue(name, durable=True):
        return await fake_declare_queue(name, durable)

    async def declare_exchange(name, ex_type, durable=True):
        return exchange_mock

    channel_mock = SimpleNamespace(declare_queue=declare_queue, declare_exchange=declare_exchange)

    async def channel():
        return channel_mock
    connection_mock = SimpleNamespace(channel=channel)

    async def connect_robust(url):
        return connection_mock

    fake_aio = SimpleNamespace(connect_robust=connect_robust, ExchangeType=SimpleNamespace(TOPIC='topic'))
    # Provide a dummy IncomingMessage for the function annotation in subscribe
    fake_aio.IncomingMessage = SimpleNamespace
    monkeypatch.setattr(rm_mod, 'aio_pika', fake_aio)
    monkeypatch.setattr(rm_mod, 'HAS_AIO_PIKA', True)

    broker = rm_mod.RabbitMQBroker(url='amqp://test/', exchange_name='ex2')

    handler_calls = []
    async def handler(payload):
        handler_calls.append(payload)

    # Subscribe (should declare queue and bind)
    await broker.subscribe('test.topic', handler)

    # queue.consume should have been called with a callable
    assert queue_calls['called'] is True
    on_message_cb = queue_calls['callbacks'][-1]

    # Simulate an incoming message
    class FakeMessage:
        def __init__(self, body):
            self.body = body
        def process(self):
            # Returning self (an object with async __aenter__/__aexit__) —
            # process must not be a coroutine otherwise `async with message.process()` fails.
            return self
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False

    m = FakeMessage(b'{"hello": "world"}')
    await on_message_cb(m)

    assert len(handler_calls) == 1
    assert handler_calls[0] == {'hello': 'world'}

    # Simulate handler that raises; should be caught and logged
    async def handler2(payload):
        raise RuntimeError('boom')
    await broker.subscribe('err.topic', handler2)
    # The consume mock was used again; get the last callback
    assert queue_calls['called'] is True
    last_cb = queue_calls['callbacks'][-1]
    await last_cb(FakeMessage(b'{"x":1}'))

    assert any('Error processing message' in rec.message for rec in caplog.records if rec.levelname == 'ERROR')
    assert any('Subscribed to' in rec.message for rec in caplog.records if rec.levelname == 'INFO')
