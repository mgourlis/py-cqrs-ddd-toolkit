
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from contextlib import contextmanager
import inspect

from cqrs_ddd.contrib.tracing import (
    TracingService,
    NoOpBackend,
    OpenTelemetryBackend,
    SentryBackend,
    TracingMiddleware,
    trace_span,
    traced_persistence,
    traced_saga,
    configure_tracing
)

def test_tracing_service_noop():
    service = TracingService() # Defaults to NoOp
    with service.start_span("test") as span:
        assert span is None

def test_tracing_service_configure():
    backend = MagicMock()
    service = TracingService()
    service.configure(backend)
    
    with service.start_span("test"):
        pass
    backend.start_span.assert_called()

@pytest.mark.asyncio
async def test_tracing_middleware():
    middleware = TracingMiddleware()
    handler = AsyncMock()
    message = MagicMock()
    
    wrapped = middleware.apply(handler, message)
    await wrapped("arg")
    
    handler.assert_awaited_with("arg")

@pytest.mark.asyncio
async def test_trace_span_decorator():
    @trace_span("custom_name")
    async def my_func():
        return 42
        
    res = await my_func()
    assert res == 42

def test_traced_persistence_decorator():
    @traced_persistence
    class MyPersistence:
        async def persist(self, obj): pass
        
    p = MyPersistence()
    # It should be wrapped (a coroutine function)
    import inspect
    assert inspect.iscoroutinefunction(p.persist)

def test_traced_saga_decorator():
    class MyEvent: pass
    
    @traced_saga
    class MySaga:
        def on_MyEvent(self, evt): pass
        on_MyEvent._saga_event_type = MyEvent
        
        async def compensate(self): pass
        
    saga = MySaga()
    assert inspect.iscoroutinefunction(saga.on_MyEvent)
    assert inspect.iscoroutinefunction(saga.compensate)

def test_configure_tracing_helper():
    with patch("cqrs_ddd.contrib.tracing.logger") as mock_log:
        configure_tracing("invalid")
        # Should fall back to NoOp
        from cqrs_ddd.contrib.tracing import _tracing_service
        assert isinstance(_tracing_service.backend, NoOpBackend)
