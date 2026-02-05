"""FastAPI integration for CQRS/DDD toolkit."""

from typing import Type, Callable, Any, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Request  # type: ignore

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    APIRouter = Any  # type: ignore
    Depends = Any  # type: ignore
    HTTPException = Any  # type: ignore
    Request = Any  # type: ignore

from ..mediator import Mediator
from ..protocols import OutboxProcessor


# =============================================================================
# Dependency Injection Hook
# =============================================================================


def configure_fastapi(container: Any) -> None:
    """
    Configure FastAPI integration with CQRS components.

    This is now a no-op as everything is resolved via the container,
    but we keep it for backward compatibility of the 'configure' step.
    """
    pass


# =============================================================================
# CQRS Router
# =============================================================================


class CQRSRouter:
    """
    Router that auto-creates endpoints for commands and queries.

    Usage:
        router = CQRSRouter(container=container, prefix="/api/v1", tags=["Users"])

        router.command("/users", CreateUser, response_model=UserResponse, tags=["Onboarding"])
        router.query("/users/{id}", GetUser, response_model=UserResponse)

        app.include_router(router.router)
    """

    def __init__(
        self,
        container: Any = None,
        mediator_provider: Any = None,
        prefix: str = "",
        tags: Optional[list[str]] = None,
        **kwargs: Any,
    ):
        if not HAS_FASTAPI:
            raise ImportError(
                "FastAPI is required. Install with: pip install py-cqrs-ddd-toolkit[fastapi]"
            )

        self.router = APIRouter(prefix=prefix, tags=tags or [], **kwargs)  # type: ignore

        if mediator_provider:
            self._mediator_dep = mediator_provider
        elif container:
            core = getattr(container, "core", container)
            self._mediator_dep = lambda: core.mediator()
        else:
            raise ValueError(
                "CQRSRouter requires either 'container' or 'mediator_provider'"
            )

    def command(
        self,
        path: str,
        command_class: Type[Any],
        response_model: Optional[Type[Any]] = None,
        status_code: int = 200,
        tags: Optional[list[str]] = None,
        response_mapper: Optional[Callable[[Any], Any]] = None,
        **kwargs: Any,
    ):
        """Register a command endpoint."""

        @self.router.post(
            path,
            response_model=response_model,
            status_code=status_code,
            tags=tags,
            **kwargs,
        )
        async def command_endpoint(
            cmd: command_class,  # type: ignore
            mediator: Mediator = Depends(self._mediator_dep),
        ):
            result = await mediator.send(cmd)
            if response_mapper:
                return response_mapper(result)
            return result

        return command_endpoint

    def query(
        self,
        path: str,
        query_class: Type[Any],
        response_model: Optional[Type[Any]] = None,
        tags: Optional[list[str]] = None,
        response_mapper: Optional[Callable[[Any], Any]] = None,
        **kwargs: Any,
    ):
        """Register a query endpoint."""

        @self.router.get(path, response_model=response_model, tags=tags, **kwargs)
        async def query_endpoint(
            query: query_class = Depends(),  # type: ignore
            mediator: Mediator = Depends(self._mediator_dep),
        ):
            result = await mediator.send(query)
            if response_mapper:
                return response_mapper(result)
            return result

        return query_endpoint


# =============================================================================
# Exception Handlers
# =============================================================================


def register_exception_handlers(app: Any) -> None:
    """
    Register CQRS exception handlers with FastAPI app.

    Usage:
        from cqrs_ddd.contrib.fastapi import register_exception_handlers
        register_exception_handlers(app)
    """
    if not HAS_FASTAPI:
        return

    from ..exceptions import (
        ValidationError,
        EntityNotFoundError,
        DomainError,
        AuthorizationError,
        ConcurrencyError,
        InfrastructureError,
        HandlerNotFoundError,
    )

    from fastapi.responses import JSONResponse

    @app.exception_handler(ValidationError)
    async def validation_error_handler(
        request: Request, exc: ValidationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": exc.to_dict()})

    @app.exception_handler(EntityNotFoundError)
    async def not_found_handler(
        request: Request, exc: EntityNotFoundError
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"detail": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(AuthorizationError)
    async def authorization_error_handler(
        request: Request, exc: AuthorizationError
    ) -> JSONResponse:
        return JSONResponse(status_code=403, content={"detail": str(exc)})

    @app.exception_handler(ConcurrencyError)
    async def concurrency_error_handler(
        request: Request, exc: ConcurrencyError
    ) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(InfrastructureError)
    async def infrastructure_error_handler(
        request: Request, exc: InfrastructureError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503,
            content={"detail": {"code": exc.code, "message": "Service unavailable"}},
        )

    @app.exception_handler(HandlerNotFoundError)
    async def handler_not_found_handler(
        request: Request, exc: HandlerNotFoundError
    ) -> JSONResponse:
        return JSONResponse(status_code=501, content={"detail": str(exc)})


# =============================================================================
# Background Service Integration
# =============================================================================


def register_outbox_worker(app: Any, worker: OutboxProcessor) -> None:
    """
    Register an OutboxWorker (or any OutboxProcessor) with FastAPI.

    This hooks the worker's start/stop methods into the app's lifecycle events.

    Usage:
        worker = container.outbox_worker()
        register_outbox_worker(app, worker)
    """
    if not HAS_FASTAPI:
        return

    async def start_outbox() -> None:
        await worker.start()

    async def stop_outbox() -> None:
        await worker.stop()

    # Prefer add_event_handler instead of @app.on_event (on_event is deprecated)
    app.add_event_handler("startup", start_outbox)
    app.add_event_handler("shutdown", stop_outbox)


def init_cqrs(app: Any, container: Any, enable_exception_handlers: bool = True) -> None:
    """
    Initialize all CQRS components for a FastAPI application.

    1. Initializes container resources (triggers auto-wiring of events).
    2. Registers exception handlers (optional).

    Usage:
        init_cqrs(app, container)
    """
    if not HAS_FASTAPI:
        return

    async def startup_container() -> None:
        # Initialize container resources (autowire_router, etc.)
        container.init_resources()

    # Register startup handler using add_event_handler for compatibility
    app.add_event_handler("startup", startup_container)

    if enable_exception_handlers:
        register_exception_handlers(app)
