
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from contextlib import contextmanager

from cqrs_ddd.contrib.tracing import (
    configure_tracing,
    OpenTelemetryBackend,
    SentryBackend,
    NoOpBackend,
    TracingMiddleware,
    instrument_dispatcher,
    instrument_saga_manager,
    _tracing_service,
    HAS_OPENTELEMETRY,
    HAS_SENTRY
)

# --- Mock Classes for OpKind Testing ---
class MockCommand: pass
class MockQuery: pass
class MockEvent: 
    correlation_id = "corr-1"
    event_id = "evt-1"

# --- Tests ---

def test_configure_tracing_custom_backend():
    """Cover custom_backend path."""
    custom = MagicMock()
    configure_tracing(custom_backend=custom)
    assert _tracing_service.backend == custom

def test_backends_missing_deps():
    """Cover ImportError paths when deps are missing."""
    with patch("cqrs_ddd.contrib.tracing.HAS_OPENTELEMETRY", False):
        with pytest.raises(ImportError, match="OpenTelemetry is not installed"):
            OpenTelemetryBackend()
            
    with patch("cqrs_ddd.contrib.tracing.HAS_SENTRY", False):
        with pytest.raises(ImportError, match="Sentry SDK is not installed"):
            SentryBackend()

def test_configure_tracing_missing_deps_fallback():
    """Cover fallback to NoOp when deps are missing."""
    with patch("cqrs_ddd.contrib.tracing.HAS_OPENTELEMETRY", False):
        configure_tracing("opentelemetry")
        assert isinstance(_tracing_service.backend, NoOpBackend)
        
    with patch("cqrs_ddd.contrib.tracing.HAS_SENTRY", False):
        configure_tracing("sentry")
        assert isinstance(_tracing_service.backend, NoOpBackend)

def test_opentelemetry_backend_execution():
    """Cover OpenTelemetry span logic including errors."""
    with patch("cqrs_ddd.contrib.tracing.HAS_OPENTELEMETRY", True):
        with patch("cqrs_ddd.contrib.tracing.trace") as mock_trace:
            tracer = mock_trace.get_tracer.return_value
            span_ctx = MagicMock()
            tracer.start_as_current_span.return_value = span_ctx
            
            backend = OpenTelemetryBackend("test-svc")
            
            # Normal path
            with backend.start_span("span1") as span:
                pass
            
            # Error path
            try:
                with backend.start_span("span_err") as span:
                    raise ValueError("trace-crash")
            except ValueError:
                pass
            
            span_ctx.__enter__.return_value.set_status.assert_called()
            span_ctx.__enter__.return_value.record_exception.assert_called()

def test_sentry_backend_execution():
    """Cover Sentry span logic including attributes."""
    with patch("cqrs_ddd.contrib.tracing.HAS_SENTRY", True):
        with patch("cqrs_ddd.contrib.tracing.sentry_sdk") as mock_sentry:
            span_mock = MagicMock()
            mock_sentry.start_span.return_value.__enter__.return_value = span_mock
            
            backend = SentryBackend()
            
            # With attributes
            with backend.start_span("sentry_span", attributes={"messaging.operation": "test-op", "attr1": "val1"}):
                pass
            
            span_mock.set_data.assert_called_with("attr1", "val1")
            
            # Error path
            try:
                with backend.start_span("sentry_err"):
                    raise ValueError("sentry-crash")
            except ValueError:
                pass

@pytest.mark.asyncio
async def test_tracing_middleware_op_kinds():
    """Cover operation kind detection in TracingMiddleware."""
    mw = TracingMiddleware()
    
    # Define base classes with expected names for MRO detection
    class Command: pass
    class Query: pass
    class DomainEvent: pass

    # 1. Command
    class RealCommand(Command): pass
    msg = RealCommand()
    async def _dummy_cmd():
        pass
    wrapped = mw.apply(_dummy_cmd, msg)
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await wrapped()
        _, kwargs = mock_start.call_args
        assert kwargs["attributes"]["messaging.operation"] == "command"

    # 2. Query
    class RealQuery(Query): pass
    msg = RealQuery()
    async def _dummy_q():
        pass
    wrapped = mw.apply(_dummy_q, msg)
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await wrapped()
        _, kwargs = mock_start.call_args
        assert kwargs["attributes"]["messaging.operation"] == "query"

    # 3. DomainEvent with IDs
    class RealEvent(DomainEvent): 
        correlation_id = "c1"
        event_id = "e1"
    msg = RealEvent()
    async def _dummy_evt():
        pass
    wrapped = mw.apply(_dummy_evt, msg)
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await wrapped()
        _, kwargs = mock_start.call_args
        attr = kwargs["attributes"]
        assert attr["messaging.operation"] == "event"
        assert attr["messaging.correlation_id"] == "c1"
        assert attr["messaging.message_id"] == "e1"

    # 4. Unknown kind
    class UnknownOp: pass
    msg = UnknownOp()
    async def _dummy_unknown():
        pass
    wrapped = mw.apply(_dummy_unknown, msg)
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await wrapped()
        _, kwargs = mock_start.call_args
        assert kwargs["attributes"]["messaging.operation"] == "unknown"

@pytest.mark.asyncio
async def test_instrumentation_helpers():
    """Cover instrument_dispatcher and instrument_saga_manager."""
    # Dispatcher
    dispatcher = MagicMock()
    async def dispatcher_apply():
        pass
    dispatcher.apply = dispatcher_apply
    instrument_dispatcher(dispatcher)
    
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await dispatcher.apply()
        mock_start.assert_called()

    # Saga Manager
    manager = MagicMock()
    manager_calls = []
    async def handle_event(e):
        manager_calls.append(e)
    manager.handle_event = handle_event
    instrument_saga_manager(manager)
    
    evt = MockEvent()
    with patch("cqrs_ddd.contrib.tracing._tracing_service.start_span") as mock_start:
        await manager.handle_event(evt)
        mock_start.assert_called()
        args, kwargs = mock_start.call_args
        assert kwargs["attributes"]["messaging.correlation_id"] == "corr-1"

def test_yield_coverage():
    """Cover start_span yields in different classes."""
    from cqrs_ddd.contrib.tracing import TracingService
    # NoOp
    noop = NoOpBackend()
    with noop.start_span("test") as span:
        assert span is None
        
    # Service
    svc = TracingService(noop)
    with svc.start_span("test") as span:
        assert span is None

def test_configure_tracing_success_paths():
    """Cover success paths for OTEL and Sentry."""
    with patch("cqrs_ddd.contrib.tracing.HAS_OPENTELEMETRY", True):
        with patch("cqrs_ddd.contrib.tracing.OpenTelemetryBackend") as mock_otel:
            configure_tracing("opentelemetry")
            assert _tracing_service.backend == mock_otel.return_value
            
    with patch("cqrs_ddd.contrib.tracing.HAS_SENTRY", True):
        with patch("cqrs_ddd.contrib.tracing.SentryBackend") as mock_sentry:
            configure_tracing("sentry")
            assert _tracing_service.backend == mock_sentry.return_value

    # Default path (noop)
    configure_tracing("unknown")
    assert isinstance(_tracing_service.backend, NoOpBackend)

def test_decorator_logic_coverage():
    """Cover traced_persistence and traced_saga internals."""
    from cqrs_ddd.contrib.tracing import traced_persistence, traced_saga
    
    @traced_persistence
    class TestRepo:
        async def persist(self): return "ok"
        async def retrieve(self): pass
        
    repo = TestRepo()
    # It should be wrapped. We can check if it's a coroutine but more importantly if it runs.
    import inspect
    assert inspect.iscoroutinefunction(repo.persist)
    
    @traced_saga
    class TestSaga:
        async def on_SomeEvent(self, e): pass
        on_SomeEvent._saga_event_type = type("SomeEvent", (), {})
        
        async def compensate(self): pass
        
    saga = TestSaga()
    # Check if they are wrapped by checking the __name__ or similar if applied via trace_span
    assert "on_SomeEvent" in saga.on_SomeEvent.__name__
