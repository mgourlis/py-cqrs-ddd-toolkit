"""Distributed tracing integration for CQRS/DDD toolkit."""

from functools import wraps
from contextlib import contextmanager
from typing import Any, Callable, Dict, Protocol, runtime_checkable
import logging

# === Imports & Availability Checks ===

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    HAS_OPENTELEMETRY = True
except ImportError:
    HAS_OPENTELEMETRY = False
    trace = Any
    Status = Any
    StatusCode = Any

try:
    import sentry_sdk

    HAS_SENTRY = True
except ImportError:
    HAS_SENTRY = False

logger = logging.getLogger("cqrs_ddd")


# === Abstraction ===


@runtime_checkable
class TracingBackend(Protocol):
    """Protocol for tracing backends."""

    def start_span(
        self, name: str, kind: Any = None, attributes: Dict[str, Any] = None
    ):
        """Start a span as a context manager."""
        ...


class NoOpBackend(TracingBackend):
    """Null Object implementation."""

    @contextmanager
    def start_span(
        self, name: str, kind: Any = None, attributes: Dict[str, Any] = None
    ):
        yield None


class OpenTelemetryBackend(TracingBackend):
    """OpenTelemetry implementation."""

    def __init__(self, service_name: str = "cqrs-ddd"):
        if not HAS_OPENTELEMETRY:
            raise ImportError("OpenTelemetry is not installed.")
        self.tracer = trace.get_tracer(service_name)

    @contextmanager
    def start_span(
        self, name: str, kind: Any = None, attributes: Dict[str, Any] = None
    ):
        with self.tracer.start_as_current_span(
            name, kind=kind, attributes=attributes or {}
        ) as span:
            try:
                yield span
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                raise


class SentryBackend(TracingBackend):
    """Sentry implementation."""

    def __init__(self):
        if not HAS_SENTRY:
            raise ImportError("Sentry SDK is not installed.")

    @contextmanager
    def start_span(
        self, name: str, kind: Any = None, attributes: Dict[str, Any] = None
    ):
        # Sentry uses start_span or start_transaction
        # We'll map 'kind' to Sentry operation if possible
        op = (
            attributes.get("messaging.operation", "unknown")
            if attributes
            else "unknown"
        )

        with sentry_sdk.start_span(op=op, name=name) as span:
            if attributes:
                for k, v in attributes.items():
                    span.set_data(k, v)
            try:
                yield span
            except Exception:
                # Sentry automatically captures exceptions in the span context,
                # but we can explicitly capture if needed, though start_span usually handles it.
                # Re-raising allows Sentry's global handler to catch it too.
                raise


# === Service ===


class TracingService:
    """Service to handle distributed tracing via configured backend."""

    def __init__(self, backend: TracingBackend = None):
        self.backend = backend or NoOpBackend()

    def configure(self, backend: TracingBackend):
        """Update the tracing backend."""
        self.backend = backend

    @contextmanager
    def start_span(
        self, name: str, kind: Any = None, attributes: Dict[str, Any] = None
    ):
        """Start a new span using the configured backend."""
        with self.backend.start_span(name, kind, attributes) as span:
            yield span


# Global instance
_tracing_service = TracingService()


def configure_tracing(
    backend_name: str = "noop",
    service_name: str = "cqrs-ddd",
    custom_backend: TracingBackend = None,
):
    """
    Configure the global tracing service.

    Args:
        backend_name: 'noop', 'opentelemetry', or 'sentry'.
        service_name: Service name for OpenTelemetry.
        custom_backend: Directly provide a backend instance.
    """
    if custom_backend:
        _tracing_service.configure(custom_backend)
        return

    if backend_name == "opentelemetry":
        if not HAS_OPENTELEMETRY:
            logger.warning("OpenTelemetry not found, falling back to NoOp.")
            _tracing_service.configure(NoOpBackend())
        else:
            _tracing_service.configure(OpenTelemetryBackend(service_name))

    elif backend_name == "sentry":
        if not HAS_SENTRY:
            logger.warning("Sentry not found, falling back to NoOp.")
            _tracing_service.configure(NoOpBackend())
        else:
            _tracing_service.configure(SentryBackend())

    else:
        _tracing_service.configure(NoOpBackend())


# === Middlewares & Decorators ===


class TracingMiddleware:
    """
    Middleware that adds distributed tracing to handlers.
    Delegates to the global _tracing_service.
    """

    def apply(self, handler_func: Callable, message: Any) -> Callable:
        async def wrapped(*args, **kwargs):
            msg_type = type(message).__name__

            # Determine operation kind (Command, Query, Event)
            op_kind = "unknown"
            if hasattr(message, "__class__"):
                if "Command" in [b.__name__ for b in message.__class__.__mro__]:
                    op_kind = "command"
                elif "Query" in [b.__name__ for b in message.__class__.__mro__]:
                    op_kind = "query"
                elif "DomainEvent" in [b.__name__ for b in message.__class__.__mro__]:
                    op_kind = "event"

            span_name = f"handle_{op_kind} {msg_type}"

            attributes = {
                "messaging.system": "cqrs-ddd",
                "messaging.operation": op_kind,
                "messaging.destination": msg_type,
            }

            # Add correlation info
            if hasattr(message, "correlation_id") and message.correlation_id:
                attributes["messaging.correlation_id"] = message.correlation_id
            if hasattr(message, "event_id") and message.event_id:
                attributes["messaging.message_id"] = message.event_id

            # OpenTelemetry specific kind mapping
            otel_kind = None
            if HAS_OPENTELEMETRY:
                otel_kind = trace.SpanKind.CONSUMER

            with _tracing_service.start_span(
                span_name, kind=otel_kind, attributes=attributes
            ):
                return await handler_func(*args, **kwargs)

        return wrapped


def trace_span(name: str = None):
    """Decorator to trace a specific function/method using the global backend."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            span_name = name or func.__name__
            with _tracing_service.start_span(span_name):
                return await func(*args, **kwargs)

        return wrapper

    return decorator


def traced_persistence(cls):
    """
    Class decorator to automatically trace standard persistence methods.

    Target methods: 'persist', 'retrieve', 'fetch_domain', 'fetch', 'try_retrieve'.
    """
    methods = [
        "persist",
        "retrieve",
        "fetch_domain",
        "fetch_domain_one",
        "fetch",
        "fetch_one",
        "retrieve_batch",
        "retrieve_one",
        "try_retrieve",
    ]

    for method_name in methods:
        if hasattr(cls, method_name):
            original_method = getattr(cls, method_name)
            span_name = f"{cls.__name__}.{method_name}"
            current_wrapper = trace_span(span_name)(original_method)
            setattr(cls, method_name, current_wrapper)

    return cls


def instrument_dispatcher(persistence_dispatcher: Any):
    """
    Instrument a PersistenceDispatcher instance with tracing.
    """
    methods = [
        "apply",
        "fetch",
        "fetch_one",
        "fetch_domain",
        "fetch_domain_one",
        "retrieve",
        "retrieve_one",
    ]

    for method_name in methods:
        if hasattr(persistence_dispatcher, method_name):
            original = getattr(persistence_dispatcher, method_name)
            span_name = f"PersistenceDispatcher.{method_name}"
            setattr(
                persistence_dispatcher, method_name, trace_span(span_name)(original)
            )
            logger.debug(f"Instrumented {span_name}")


def traced_saga(cls):
    """
    Class decorator to automatically trace methods decorated with @saga_step.
    """
    for name in dir(cls):
        if name.startswith("__"):
            continue
        try:
            attr = getattr(cls, name)
            if hasattr(attr, "_saga_event_type"):
                event_type = attr._saga_event_type
                span_name = f"saga:{cls.__name__}.on_{event_type.__name__}"
                setattr(cls, name, trace_span(span_name)(attr))
        except Exception:
            continue

    # Also trace compensation
    if hasattr(cls, "compensate"):
        original = getattr(cls, "compensate")
        setattr(
            cls, "compensate", trace_span(f"saga:{cls.__name__}.compensate")(original)
        )

    return cls


def instrument_saga_manager(saga_manager: Any):
    """
    Instrument a SagaManager instance with tracing by wrapping its handle_event method.
    """
    original_handle = saga_manager.handle_event

    @wraps(original_handle)
    async def traced_handle(event):
        event_type = type(event).__name__
        span_name = f"saga_manager:handle_{event_type}"

        attributes = {
            "messaging.operation": "orchestration",
            "messaging.destination": event_type,
        }
        if hasattr(event, "correlation_id") and event.correlation_id:
            attributes["messaging.correlation_id"] = event.correlation_id

        with _tracing_service.start_span(span_name, attributes=attributes):
            return await original_handle(event)

    saga_manager.handle_event = traced_handle
    logger.debug("Instrumented SagaManager.handle_event with tracing")
