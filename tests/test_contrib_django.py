
import pytest
import json
from unittest.mock import MagicMock, patch, AsyncMock

try:
    from cqrs_ddd.contrib.django import (
        CQRSView, 
        CQRSExceptionMiddleware,
        run_outbox_worker
    )
    from django.http import JsonResponse, HttpRequest
    HAS_DJANGO = True
except ImportError:
    HAS_DJANGO = False

@pytest.fixture(autouse=True)
def setup_django_settings():
    if not HAS_DJANGO:
        return
    from django.conf import settings
    if not settings.configured:
        settings.configure(DEBUG=True, SECRET_KEY="secret", DEFAULT_CHARSET="utf-8")

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
@pytest.mark.asyncio
async def test_cqrs_view_dispatch():
    my_container = MagicMock()
    mediator = AsyncMock()
    # mediator.send must be an AsyncMock
    mediator.send = AsyncMock(return_value="Success")
    # core.mediator() returns the mediator instance
    my_container.mediator = MagicMock(return_value=mediator)
    my_container.core = my_container # For getattr(self.container, 'core', self.container)
    
    class MyView(CQRSView):
        container = my_container
        
    view = MyView()
    
    # Test dispatch
    res = await view.dispatch_command("cmd")
    assert res == "Success"
    mediator.send.assert_awaited_with("cmd")
    
    res = await view.dispatch_query("query")
    assert res == "Success"

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
def test_cqrs_view_helpers():
    view = CQRSView()
    
    # Parse body
    req = MagicMock()
    req.body = b'{"a": 1}'
    data = view.parse_body(req)
    assert data == {"a": 1}
    
    # Success response
    resp = view.success({"id": 1})
    assert resp.status_code == 200
    assert json.loads(resp.content) == {"success": True, "result": {"id": 1}}
    
    # Error response
    resp = view.error("Bad stuff")
    assert resp.status_code == 400
    assert json.loads(resp.content)["error"]["message"] == "Bad stuff"

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
def test_exception_middleware():
    from cqrs_ddd.exceptions import (
        ValidationError, EntityNotFoundError, DomainError, 
        AuthorizationError, ConcurrencyError, InfrastructureError, HandlerNotFoundError
    )
    
    get_response = MagicMock()
    middleware = CQRSExceptionMiddleware(get_response)
    
    req = MagicMock()
    
    # Validation Error
    resp = middleware.process_exception(req, ValidationError(
        errors={"field": ["err"]},
        message_type="Cmd"
    ))
    assert resp.status_code == 422
    assert "field" in json.loads(resp.content)["detail"]["errors"]

    # Not Found
    resp = middleware.process_exception(req, EntityNotFoundError("Not found", entity_id=123))
    assert resp.status_code == 404
    
    # Domain Error
    resp = middleware.process_exception(req, DomainError("code", "msg"))
    assert resp.status_code == 400
    
    # Auth
    resp = middleware.process_exception(req, AuthorizationError("Forbid"))
    assert resp.status_code == 403

    # Concurrency
    resp = middleware.process_exception(req, ConcurrencyError(expected=1, actual=2))
    assert resp.status_code == 409
    
    # Infra
    resp = middleware.process_exception(req, InfrastructureError(code="INFRA_ERROR", message="Down"))
    assert resp.status_code == 503
    
    # Handler
    class MyCmd: pass
    resp = middleware.process_exception(req, HandlerNotFoundError(MyCmd))
    assert resp.status_code == 501
    
    # Normal exception (returns None, let Django handle)
    resp = middleware.process_exception(req, ValueError("Boom"))
    assert resp is None

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
def test_run_outbox_worker_sync():
    worker = AsyncMock()
    
    # Mock asyncio.run and time.sleep to simulate run and interrupt
    with patch("asyncio.run") as mock_run:
        with patch("time.sleep", side_effect=KeyboardInterrupt):
             run_outbox_worker(worker)
             
        # Should start, then sleep (raise), then stop
        assert mock_run.call_count >= 2 # start and stop
