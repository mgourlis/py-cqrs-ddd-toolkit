import pytest
from unittest.mock import MagicMock, AsyncMock, patch

try:
    from fastapi import FastAPI
    from pydantic import BaseModel
    from cqrs_ddd.contrib.fastapi import (
        CQRSRouter,
        init_cqrs,
        register_outbox_worker,
    )

    pass

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


@pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
def test_cqrs_router():
    # Mock container
    container = MagicMock()

    # Init router
    router = CQRSRouter(container=container)

    # Mock classes
    class Cmd(BaseModel):
        pass

    class Query(BaseModel):
        pass

    class Resp(BaseModel):
        pass

    # Register command
    router.command("/cmd", Cmd, response_model=Resp)

    # Register query
    router.query("/query", Query, response_model=Resp)

    # Verify routes added
    routes = router.router.routes
    assert len(routes) == 2
    assert routes[0].path == "/cmd"
    assert routes[0].methods == {"POST"}
    assert routes[1].path == "/query"
    assert routes[1].methods == {"GET"}


@pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
def test_init_cqrs():
    app = FastAPI()
    container = MagicMock()

    # Mock exception handler registration
    with patch("cqrs_ddd.contrib.fastapi.register_exception_handlers") as mock_reg:
        init_cqrs(app, container)
        mock_reg.assert_called_once()

    # Check startup event
    # FastAPI keeps handlers in router.on_startup list (deprecated) or lifespan (new)
    # But for older versions it's in app.router.on_startup
    assert len(app.router.on_startup) == 1


@pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
def test_register_outbox_worker():
    app = FastAPI()
    worker = AsyncMock()

    register_outbox_worker(app, worker)

    assert len(app.router.on_startup) == 1
    assert len(app.router.on_shutdown) == 1
