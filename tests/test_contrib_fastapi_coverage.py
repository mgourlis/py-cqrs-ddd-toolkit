
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from pydantic import BaseModel

from cqrs_ddd.contrib.fastapi import (
    CQRSRouter, 
    init_cqrs, 
    register_exception_handlers,
    register_outbox_worker,
    configure_fastapi
)
from cqrs_ddd.exceptions import (
    ValidationError, EntityNotFoundError, DomainError, 
    AuthorizationError, ConcurrencyError, InfrastructureError,
    HandlerNotFoundError
)

# --- Helpers ---

class MockCmd(BaseModel):
    name: str

class MockQuery(BaseModel):
    id: str

# --- Tests ---

def test_configure_fastapi():
    """Cover the no-op configure_fastapi."""
    configure_fastapi(None)

def test_cqrs_router_init_variations():
    """Cover different init paths for CQRSRouter."""
    # 1. mediator_provider
    mediator = MagicMock()
    router = CQRSRouter(mediator_provider=lambda: mediator)
    assert router._mediator_dep() == mediator
    
    # 2. container with .core
    container = MagicMock()
    core = MagicMock()
    container.core = core
    core.mediator = lambda: mediator
    router = CQRSRouter(container=container)
    assert router._mediator_dep() == mediator
    
    # 3. ValueError when both None
    with pytest.raises(ValueError, match="requires either 'container' or 'mediator_provider'"):
        CQRSRouter(container=None, mediator_provider=None)

def test_cqrs_router_endpoints_coverage():
    """Cover endpoint execution and mapping."""
    app = FastAPI()
    mediator = AsyncMock()
    router = CQRSRouter(mediator_provider=lambda: mediator)
    
    # Command with mapper
    router.command(
        "/cmd-mapped", 
        MockCmd, 
        response_mapper=lambda r: {"mapped": r}
    )
    
    # Command without mapper
    router.command("/cmd", MockCmd)
    
    # Query with mapper
    router.query(
        "/query-mapped", 
        MockQuery, 
        response_mapper=lambda r: {"mapped": r}
    )
    
    # Query without mapper
    router.query("/query", MockQuery)
    
    app.include_router(router.router)
    client = TestClient(app)
    
    # Execute Command (Mapped)
    mediator.send.return_value = "success"
    resp = client.post("/cmd-mapped", json={"name": "test"})
    assert resp.status_code == 200
    assert resp.json() == {"mapped": "success"}
    
    # Execute Command (Not Mapped)
    resp = client.post("/cmd", json={"name": "test"})
    assert resp.status_code == 200
    assert resp.json() == "success"
    
    # Execute Query (Mapped)
    mediator.send.return_value = "data"
    resp = client.get("/query-mapped?id=1")
    assert resp.status_code == 200
    assert resp.json() == {"mapped": "data"}
    
    # Execute Query (Not Mapped)
    resp = client.get("/query?id=1")
    assert resp.status_code == 200
    assert resp.json() == "data"

def test_exception_handlers_full_coverage():
    """Exercise all registered exception handlers."""
    app = FastAPI()
    register_exception_handlers(app)
    client = TestClient(app)
    
    @app.get("/validation-error")
    async def raise_validation():
        raise ValidationError("Command", {"field": ["invalid"]})
        
    @app.get("/not-found")
    async def raise_not_found():
        raise EntityNotFoundError("User", "1")
        
    @app.get("/domain-error")
    async def raise_domain():
        raise DomainError("ORDER_LOCKED", "Cannot modify locked order")
        
    @app.get("/auth-error")
    async def raise_auth():
        raise AuthorizationError("Denied")
        
    @app.get("/concurrency-error")
    async def raise_concurrency():
        raise ConcurrencyError(1, 2)
        
    @app.get("/infra-error")
    async def raise_infra():
        raise InfrastructureError("DB_DOWN", "Database unreachable")
        
    @app.get("/no-handler")
    async def raise_no_handler():
        class MissingCmd: pass
        raise HandlerNotFoundError(MissingCmd)

    # Verify each mapping
    assert client.get("/validation-error").status_code == 422
    assert client.get("/not-found").status_code == 404
    assert client.get("/domain-error").status_code == 400
    assert client.get("/auth-error").status_code == 403
    assert client.get("/concurrency-error").status_code == 409
    
    resp = client.get("/infra-error")
    assert resp.status_code == 503
    assert "Service unavailable" in resp.json()["detail"]["message"]
    
    assert client.get("/no-handler").status_code == 501

@pytest.mark.asyncio
async def test_lifecycle_hooks_execution():
    """Cover the actual execution of startup/shutdown hooks."""
    app = FastAPI()
    container = MagicMock()
    worker = AsyncMock()
    
    init_cqrs(app, container)
    register_outbox_worker(app, worker)
    
    # Simulate startup
    for event_handler in app.router.on_startup:
        await event_handler()
        
    container.init_resources.assert_called_once()
    worker.start.assert_awaited_with()
    
    # Simulate shutdown
    for event_handler in app.router.on_shutdown:
        await event_handler()
        
    worker.stop.assert_awaited_with()

def test_import_fail_guards():
    """Cover branches where FastAPI is missing (simulated)."""
    with patch("cqrs_ddd.contrib.fastapi.HAS_FASTAPI", False):
        # register_exception_handlers should return early
        assert register_exception_handlers(None) is None
        
        # register_outbox_worker should return early
        assert register_outbox_worker(None, None) is None
        
        # init_cqrs should return early
        assert init_cqrs(None, None) is None
        
        # CQRSRouter should raise ImportError
        with pytest.raises(ImportError):
            CQRSRouter()
