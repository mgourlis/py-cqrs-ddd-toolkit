"""Django integration for CQRS/DDD toolkit."""

from typing import Optional, Any, Type
import json

try:
    from django.http import JsonResponse, HttpRequest, HttpResponse
    from django.views import View

    HAS_DJANGO = True
except ImportError:
    HAS_DJANGO = False
    JsonResponse = Any
    HttpRequest = Any
    HttpResponse = Any
    View = object

from ..mediator import Mediator
from ..protocols import OutboxProcessor


# =============================================================================
# Dependency Injection Hook
# =============================================================================


def configure_django(container: Any) -> None:
    """
    Configure Django integration with CQRS components.

    This is now a no-op as everything is resolved via the container,
    but we keep it for backward compatibility of the 'configure' step.
    """
    pass


# =============================================================================
# Base Views
# =============================================================================

if HAS_DJANGO:

    class CQRSView(View):
        """
        Base view for CQRS operations.

        Usage:
            class MyView(CQRSView):
                container = DemoContainer()
        """

        container: Any = None
        _mediator: Optional[Mediator] = None

        @property
        def mediator(self) -> Mediator:
            if self._mediator:
                return self._mediator
            if self.container:
                core = getattr(self.container, "core", self.container)
                return core.mediator()
            raise RuntimeError(
                "CQRSView requires either 'container' or '_mediator' to be set."
            )

        async def dispatch_command(self, command: Any) -> Any:
            """Dispatch a command with its UoW scope (managed by Mediator)."""
            return await self.mediator.send(command)

        async def dispatch_query(self, query: Any) -> Any:
            """Dispatch a query."""
            return await self.mediator.send(query)

        def parse_body(self, request: HttpRequest, model_class: Type = None) -> Any:
            """Parse JSON body, optionally into a Pydantic model."""
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError:
                data = {}

            if model_class:
                return model_class(**data)
            return data

        def success(self, data: Any = None, status: int = 200) -> JsonResponse:
            """Return success JSON response."""
            response = {"success": True}
            if data is not None:
                response["result"] = data
            return JsonResponse(response, status=status)

        def error(
            self, message: str, code: str = "ERROR", status: int = 400
        ) -> JsonResponse:
            """Return error JSON response."""
            return JsonResponse(
                {"success": False, "error": {"code": code, "message": message}},
                status=status,
            )

else:

    class CQRSView:
        def __init__(self, *args, **kwargs):
            raise ImportError("Django is not installed.")


# =============================================================================
# Exception Middleware
# =============================================================================

if HAS_DJANGO:
    from ..exceptions import (
        ValidationError,
        EntityNotFoundError,
        DomainError,
        AuthorizationError,
        ConcurrencyError,
        InfrastructureError,
        HandlerNotFoundError,
    )

    class CQRSExceptionMiddleware:
        """
        Middleware to handle CQRS exceptions and return JSON responses.

        Supports both sync and async via standard Django middleware patterns.
        """

        def __init__(self, get_response):
            self.get_response = get_response

        def __call__(self, request):
            try:
                response = self.get_response(request)
                return response
            except Exception as exc:
                result = self._handle(exc)
                if result:
                    return result
                raise exc

        async def __acall__(self, request):
            try:
                response = await self.get_response(request)
                return response
            except Exception as exc:
                # We need to map exception in async context
                # _handle is sync but creating JsonResponse is safe
                result = self._handle(exc)
                if result:
                    return result
                raise exc

        def process_exception(self, request, exception):
            """Legacy sync hook."""
            return self._handle(exception)

        def _handle(self, exc):
            """Map exceptions to responses."""
            if isinstance(exc, ValidationError):
                return JsonResponse({"detail": exc.to_dict()}, status=422)
            elif isinstance(exc, EntityNotFoundError):
                return JsonResponse({"detail": str(exc)}, status=404)
            elif isinstance(exc, DomainError):
                return JsonResponse(
                    {"detail": {"code": exc.code, "message": exc.message}}, status=400
                )
            elif isinstance(exc, AuthorizationError):
                return JsonResponse({"detail": str(exc)}, status=403)
            elif isinstance(exc, ConcurrencyError):
                return JsonResponse({"detail": str(exc)}, status=409)
            elif isinstance(exc, InfrastructureError):
                return JsonResponse(
                    {"detail": {"code": exc.code, "message": "Service unavailable"}},
                    status=503,
                )
            elif isinstance(exc, HandlerNotFoundError):
                return JsonResponse({"detail": str(exc)}, status=501)
            return None


# =============================================================================
# Background Service Integration
# =============================================================================


def run_outbox_worker(worker: OutboxProcessor):
    """
    Run an OutboxWorker loop synchronously.

    This is best used in a custom Django Management Command.

    Usage (in management/commands/run_outbox.py):
        from cqrs_ddd.contrib.django import run_outbox_worker

        class Command(BaseCommand):
            def handle(self, *args, **options):
                worker = container.outbox_worker()
                run_outbox_worker(worker)
    """
    import asyncio

    try:
        asyncio.run(worker.start())
        # The worker.start() creates a task, so we need to wait
        # In a management command, we usually want to run until interrupted
        while True:
            import time

            time.sleep(1)
    except KeyboardInterrupt:
        asyncio.run(worker.stop())
