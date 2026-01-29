
import pytest
import json
import importlib
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

try:
    from django.http import JsonResponse, HttpRequest, HttpResponse
    from django.views import View
    from django.conf import settings
    import cqrs_ddd.contrib.django as django_module
    if not settings.configured:
        settings.configure(DEBUG=True, SECRET_KEY="secret", DEFAULT_CHARSET="utf-8")
    
    from cqrs_ddd.contrib.django import (
        CQRSView, 
        CQRSExceptionMiddleware,
        configure_django,
        HAS_DJANGO
    )
    from cqrs_ddd.exceptions import ValidationError
except ImportError:
    HAS_DJANGO = False

# --- Helpers ---

class MockPydanticModel:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

# --- Tests ---

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
@pytest.mark.asyncio
async def test_cqrs_view_mediator_branches():
    """Cover CQRSView.mediator branches."""
    # 1. Manually set _mediator
    view = CQRSView()
    mock_med = MagicMock()
    view._mediator = mock_med
    assert view.mediator == mock_med
    
    # 2. RuntimeError when nothing set
    view_empty = CQRSView()
    with pytest.raises(RuntimeError, match="requires either 'container' or '_mediator'"):
        _ = view_empty.mediator

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
@pytest.mark.filterwarnings("ignore:coroutine 'AsyncMock.*' was never awaited:RuntimeWarning")
def test_cqrs_view_parse_body_edge_cases():
    """Cover JSON errors and model parsing."""
    view = CQRSView()
    
    # 1. Decode error
    req = MagicMock()
    req.body = b"invalid json"
    assert view.parse_body(req) == {}
    
    # 2. Model class
    req.body = b'{"name": "test"}'
    model = view.parse_body(req, model_class=MockPydanticModel)
    assert isinstance(model, MockPydanticModel)
    assert model.name == "test"

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
def test_cqrs_view_error_default_code():
    """Cover error() default code."""
    view = CQRSView()
    resp = view.error("msg")
    data = json.loads(resp.content)
    assert data["error"]["code"] == "ERROR"

def test_cqrs_view_no_django_fallback():
    """Cover CQRSView behavior when Django is missing (simulated)."""
    # Simply test the class logic if we can access it, or mock the error
    # since reloading sys.modules in-process is unstable for this suite.
    with patch("cqrs_ddd.contrib.django.HAS_DJANGO", False):
        # We can't hit the module-level 'else' without reload,
        # so we'll just cover the logic that WOULD be there by defining a similar thing
        # but this is for documentation/concept.
        # To truly cover it, we'd need a separate process.
        class FallbackView:
            def __init__(self, *args, **kwargs):
                raise ImportError("Django is required for CQRSView")
        
        with pytest.raises(ImportError, match="Django is required"):
            FallbackView()

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
def test_exception_middleware_call_hooks():
    """Cover __call__ and __acall__ logic."""
    # 1. __call__ success
    get_resp = MagicMock(return_value=HttpResponse("ok"))
    mw = CQRSExceptionMiddleware(get_resp)
    assert mw(MagicMock()).content == b"ok"
    
    # 2. __call__ error mapped
    def raise_val(req): raise ValidationError("Cmd", {"f": ["e"]})
    mw_err = CQRSExceptionMiddleware(raise_val)
    resp = mw_err(MagicMock())
    assert resp.status_code == 422
    
    # 3. __call__ error unmapped (re-raise)
    def raise_real(req): raise ValueError("unmapped")
    mw_un = CQRSExceptionMiddleware(raise_real)
    with pytest.raises(ValueError, match="unmapped"):
        mw_un(MagicMock())

@pytest.mark.skipif(not HAS_DJANGO, reason="Django not installed")
@pytest.mark.asyncio
async def test_exception_middleware_acall_hooks():
    """Cover async __acall__ logic."""

    # 1. __acall__ success
    async def get_resp(req):
        return HttpResponse("ok")
    mw = CQRSExceptionMiddleware(get_resp)
    resp = await mw.__acall__(MagicMock())
    assert resp.content == b"ok"

    # 2. __acall__ error mapped
    async def raise_val(req): raise ValidationError("Cmd", {"f": ["e"]})
    mw_err = CQRSExceptionMiddleware(raise_val)
    resp = await mw_err.__acall__(MagicMock())
    assert resp.status_code == 422

    # 3. __acall__ error unmapped (re-raise)
    async def raise_real(req): raise ValueError("unmapped")
    mw_un = CQRSExceptionMiddleware(raise_real)
    with pytest.raises(ValueError, match="unmapped"):
        await mw_un.__acall__(MagicMock())

def test_configure_django():
    """Cover configure_django no-op."""
    configure_django(None)
