import pytest
from unittest.mock import patch
from cqrs_ddd.core import (
    CommandHandler, 
    QueryHandler, 
    EventHandler,
    AbstractCommand,
    AbstractQuery
)

# --- Tests ---

def test_command_handler_subclass_warnings():
    with patch("cqrs_ddd.core.logger") as mock_log:
        
        # 1. No handle method (from ABC, but let's assume we skip ABC check by mixin?)
        # Actually ABC enforce it. But __init_subclass__ runs before ABCMeta check?
        # No, ABC check happens at instantiation usually or strict check.
        # But auto-register logic checks for 'handle'.
        
        # 2. handle() with no type hint
        class BadHandler1(CommandHandler): # type: ignore
            async def handle(self, cmd): pass
            
        mock_log.warning.assert_any_call(
            "Could not register BadHandler1: First argument of handle() must have a type annotation"
        )
        
        # 3. handle() with wrong type hint
        class BadHandler2(CommandHandler): # type: ignore
            async def handle(self, cmd: str): pass
             
        mock_log.warning.assert_any_call(
            "Could not register BadHandler2: str does not inherit from AbstractCommand"
        )
        
        # 4. handle() with no args
        class BadHandler3(CommandHandler): # type: ignore
            async def handle(self): pass

        # This depends on inspect.signature. 
        # (self) counts as param if unbound?
        # inspect.signature on class method usually includes self? 
        # Wait, inside __init_subclass__, the class is formed.
        # methods are functions.
        
        # We need to verify what the logic does: "if len(params) >= 2"
        # self is 1st.
        
        mock_log.warning.assert_any_call(
            "Could not register BadHandler3: handle() method must accept at least one argument (besides self)"
        )

def test_query_handler_subclass_warnings():
    with patch("cqrs_ddd.core.logger") as mock_log:
        class BadQueryHandler(QueryHandler): # type: ignore
            async def handle(self, q: str): pass
            
        mock_log.warning.assert_any_call(
            "Could not register BadQueryHandler: str does not inherit from AbstractQuery"
        )

def test_event_handler_subclass_empty_union():
    # If using Union but empty? Or no type hint?
    # Logic in core.py handles Union/get_args.
    pass 
    
    # Just generic check
    with patch("cqrs_ddd.core.logger") as mock_log:
         class BadEventHandler(EventHandler): # type: ignore
             async def handle(self, evt): pass
             
         mock_log.warning.assert_called()

def test_abc_subclass_ignored():
    # Should not trigger registration or warning
    from cqrs_ddd.core import ABC
    
    with patch("cqrs_ddd.core.logger") as mock_log:
        class MyAbstractHandler(CommandHandler, ABC):
            pass
        
        assert not mock_log.warning.called

def test_base_init_coverage():
    # Cover the pass statements in __init__ via subclasses
    class ConcreteCmdHandler(CommandHandler):
        async def handle(self, cmd): pass
        
    class ConcreteQueryHandler(QueryHandler):
        async def handle(self, q): pass

    c_handler = ConcreteCmdHandler()
    q_handler = ConcreteQueryHandler()
    
    assert c_handler
    assert q_handler

def test_auto_register_exception_handling():
    # Force an exception during inspection
    with patch("inspect.signature", side_effect=ValueError("Boom")):
        with patch("cqrs_ddd.core.logger") as mock_log:
            class BrokenInspectHandler(CommandHandler):
                async def handle(self, cmd: AbstractCommand): pass
                
            mock_log.warning.assert_any_call("Error during auto-registration of BrokenInspectHandler: Boom")
