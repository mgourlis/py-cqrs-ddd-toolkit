import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from dataclasses import dataclass

from cqrs_ddd.dispatcher import EventDispatcher

# --- Helpers ---

@dataclass
class UserCreated:
    username: str

class MyEventHandler:
    def __init__(self):
        self.handled = []
        
    async def handle(self, event):
        self.handled.append(event)

class FailingHandler:
    async def handle(self, event):
        raise ValueError("Boom!")

# --- Tests ---

@pytest.mark.asyncio
async def test_manual_registration_priority():
    dispatcher = EventDispatcher()
    handler = MyEventHandler()
    
    dispatcher.register(UserCreated, handler, priority=True)
    
    # Check internal storage
    assert handler in dispatcher._priority_subscribers[UserCreated]
    assert UserCreated not in dispatcher._subscribers

@pytest.mark.asyncio
async def test_manual_registration_background():
    dispatcher = EventDispatcher()
    handler = MyEventHandler()
    
    dispatcher.register(UserCreated, handler, priority=False)
    
    assert handler in dispatcher._subscribers[UserCreated]
    assert UserCreated not in dispatcher._priority_subscribers

@pytest.mark.asyncio
async def test_dispatch_priority_success():
    dispatcher = EventDispatcher()
    handler = MyEventHandler()
    dispatcher.register(UserCreated, handler, priority=True)
    
    event = UserCreated("alice")
    await dispatcher.dispatch_priority(event)
    
    assert len(handler.handled) == 1
    assert handler.handled[0] == event

@pytest.mark.asyncio
async def test_dispatch_priority_propagates_error():
    dispatcher = EventDispatcher()
    handler = FailingHandler()
    dispatcher.register(UserCreated, handler, priority=True)
    
    event = UserCreated("bob")
    
    with pytest.raises(ValueError, match="Boom!"):
        await dispatcher.dispatch_priority(event)

@pytest.mark.asyncio
async def test_dispatch_background_fire_and_forget():
    # We need to ensure background tasks run. 
    # Since they are fire-and-forget tasks, we might need a small sleep 
    # or use a mock that allows us to await them manually if we could capture them.
    
    dispatcher = EventDispatcher()
    handler = MyEventHandler()
    dispatcher.register(UserCreated, handler, priority=False)
    
    event = UserCreated("charlie")
    
    # This should return immediately, scheduling a task
    await dispatcher.dispatch_background(event)
    
    # Give the loop a chance to run the task
    await asyncio.sleep(0.01)
    
    assert len(handler.handled) == 1
    assert handler.handled[0] == event

@pytest.mark.asyncio
async def test_dispatch_background_suppresses_error():
    dispatcher = EventDispatcher()
    handler = FailingHandler()
    dispatcher.register(UserCreated, handler, priority=False)
    
    event = UserCreated("dave")
    
    # Should NOT raise exception, but log it
    with patch("cqrs_ddd.dispatcher.logger") as mock_logger:
        await dispatcher.dispatch_background(event)
        await asyncio.sleep(0.01)
        
        mock_logger.error.assert_called_once()
        assert "failed for UserCreated" in str(mock_logger.error.call_args)

@pytest.mark.asyncio
async def test_concurrency_limiting():
    # Mock semaphore to verify acquisition
    mock_semaphore = MagicMock()
    # Context manager mock
    mock_semaphore.__aenter__ = AsyncMock()
    mock_semaphore.__aexit__ = AsyncMock()
    
    dispatcher = EventDispatcher(max_concurrent=5)
    dispatcher._semaphore = mock_semaphore
    
    handler = MyEventHandler()
    dispatcher.register(UserCreated, handler, priority=True)
    
    await dispatcher.dispatch_priority(UserCreated("eve"))
    
    mock_semaphore.__aenter__.assert_called_once()

@pytest.mark.asyncio
async def test_autodiscovery():
    # Mock registry
    with patch("cqrs_ddd.handler_registry.get_registered_handlers") as mock_get_reg:
        # Define mock structure
        # registry returns {'events': {EventType: {'priority': [Cls], 'background': [Cls]}}}
        
        class AutoPriorityHandler:
            async def handle(self, e): pass
            
        class AutoBackgroundHandler:
            async def handle(self, e): pass
            
        mock_get_reg.return_value = {
            'events': {
                UserCreated: {
                    'priority': [AutoPriorityHandler],
                    'background': [AutoBackgroundHandler]
                }
            }
        }
        
        dispatcher = EventDispatcher()
        
        # Trigger discovery by trying to dispatch (or manually calling _discover)
        # _discover_handlers is called inside dispatch methods
        
        # We need to spy on 'register' to see if it was called
        with patch.object(dispatcher, 'register', wraps=dispatcher.register) as mock_register:
            await dispatcher.dispatch_priority(UserCreated("frank"))
            
            # Should have registered both?
            # actually _discover_handlers iterates and registers.
            
            # Check internal state
            # Note: dispatch_priority calls _discover_handlers, which registers BOTH priority and background handlers for that event type
            
            # AutoPriorityHandler should be in _priority_subscribers
            # But stored as instance
            assert any(isinstance(h, AutoPriorityHandler) for h in dispatcher._priority_subscribers[UserCreated])
            
            # AutoBackgroundHandler should be in _subscribers
            assert any(isinstance(h, AutoBackgroundHandler) for h in dispatcher._subscribers[UserCreated])

@pytest.mark.asyncio
async def test_clear_subscribers():
    dispatcher = EventDispatcher()
    dispatcher.register(UserCreated, MyEventHandler(), priority=True)
    dispatcher.register(UserCreated, MyEventHandler(), priority=False)
    
    dispatcher.clear_subscribers()
    
    assert len(dispatcher._priority_subscribers) == 0
    assert len(dispatcher._subscribers) == 0
