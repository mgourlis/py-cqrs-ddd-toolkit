import pytest
import logging
from unittest.mock import patch, MagicMock

from cqrs_ddd import handler_registry
from cqrs_ddd.handler_registry import (
    register_command_handler,
    register_query_handler,
    register_event_handler,
    get_registered_handlers,
    _command_handlers,
    _query_handlers,
    _event_handlers,
)

# --- Fixture to Reset Registry ---

@pytest.fixture(autouse=True)
def reset_registry():
    """Clear the registry before and after each test."""
    _command_handlers.clear()
    _query_handlers.clear()
    _event_handlers.clear()
    yield
    _command_handlers.clear()
    _query_handlers.clear()
    _event_handlers.clear()


# --- Test Logic ---

def test_register_command_handler():
    class Cmd: pass
    class Hdlr: pass
    
    register_command_handler(Cmd, Hdlr)
    
    assert Cmd in _command_handlers
    assert _command_handlers[Cmd] == Hdlr


def test_register_command_handler_none_type():
    with patch("cqrs_ddd.handler_registry.logger") as mock_logger:
        register_command_handler(None, None)
        
        # Should log warning and not crash
        assert len(_command_handlers) == 0
        mock_logger.warning.assert_called_once()
        assert "command_type is None" in mock_logger.warning.call_args[0][0]


def test_register_query_handler():
    class Qry: pass
    class Hdlr: pass
    
    register_query_handler(Qry, Hdlr)
    
    assert Qry in _query_handlers
    assert _query_handlers[Qry] == Hdlr


def test_register_query_handler_none_type():
    with patch("cqrs_ddd.handler_registry.logger") as mock_logger:
        register_query_handler(None, None)
        
        assert len(_query_handlers) == 0
        mock_logger.warning.assert_called_once()
        assert "query_type is None" in mock_logger.warning.call_args[0][0]


def test_register_event_handler_background_default():
    class Evt: pass
    class Hdlr: pass
    
    register_event_handler(Evt, Hdlr)
    
    assert Evt in _event_handlers
    assert Hdlr in _event_handlers[Evt]['background']
    assert len(_event_handlers[Evt]['priority']) == 0


def test_register_event_handler_priority():
    class Evt: pass
    class Hdlr: pass
    
    register_event_handler(Evt, Hdlr, priority=True)
    
    assert Evt in _event_handlers
    assert Hdlr in _event_handlers[Evt]['priority']
    assert len(_event_handlers[Evt]['background']) == 0


def test_register_multiple_event_handlers():
    class Evt: pass
    class H1: pass
    class H2: pass
    class H3: pass
    
    register_event_handler(Evt, H1, priority=True)
    register_event_handler(Evt, H2, priority=False)
    register_event_handler(Evt, H3, priority=False)
    
    assert _event_handlers[Evt]['priority'] == [H1]
    assert _event_handlers[Evt]['background'] == [H2, H3]


def test_register_event_handler_none_type():
    with patch("cqrs_ddd.handler_registry.logger") as mock_logger:
        register_event_handler(None, None)
        
        assert len(_event_handlers) == 0
        mock_logger.warning.assert_called_once()
        assert "event_type is None" in mock_logger.warning.call_args[0][0]


def test_get_registered_handlers():
    class Cmd: pass
    class Qry: pass
    class Evt: pass
    class H: pass
    
    register_command_handler(Cmd, H)
    register_query_handler(Qry, H)
    register_event_handler(Evt, H)
    
    handlers = get_registered_handlers()
    
    assert handlers['commands'] == {Cmd: H}
    assert handlers['queries'] == {Qry: H}
    assert handlers['events'] == {Evt: {'priority': [], 'background': [H]}}
    
    # Assert modifications to return value don't affect internal state
    handlers['commands'].clear()
    assert Cmd in _command_handlers
