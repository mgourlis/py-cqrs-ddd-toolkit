import pytest
from dataclasses import dataclass
from typing import Dict, Any, Optional
from cqrs_ddd.event_registry import EventTypeRegistry

# --- Helpers ---

@dataclass
class SimpleEvent:
    data: str

@dataclass
class ComplexEvent:
    val: int
    
    @classmethod
    def from_dict(cls, data):
        return cls(val=int(data['val']))

# Mock StoredEvent interface
@dataclass
class MockStoredEvent:
    event_type: str
    payload: Dict[str, Any]

# --- Tests ---

@pytest.fixture(autouse=True)
def clear_registry():
    EventTypeRegistry.clear()
    yield
    EventTypeRegistry.clear()

def test_manual_registration():
    EventTypeRegistry.register_class(SimpleEvent)
    assert EventTypeRegistry.has("SimpleEvent")
    assert EventTypeRegistry.get("SimpleEvent") == SimpleEvent
    assert "SimpleEvent" in EventTypeRegistry.list_registered()

def test_decorator_registration():
    @EventTypeRegistry.register
    class DecEvent:
        pass
    
    assert EventTypeRegistry.has("DecEvent")
    assert EventTypeRegistry.get("DecEvent") == DecEvent

def test_hydrate_simple():
    EventTypeRegistry.register_class(SimpleEvent)
    
    stored = MockStoredEvent(event_type="SimpleEvent", payload={"data": "test"})
    event = EventTypeRegistry.hydrate(stored)
    
    assert isinstance(event, SimpleEvent)
    assert event.data == "test"

def test_hydrate_from_dict_method():
    EventTypeRegistry.register_class(ComplexEvent)
    
    stored = MockStoredEvent(event_type="ComplexEvent", payload={"val": "123"}) # String payload
    event = EventTypeRegistry.hydrate(stored)
    
    assert isinstance(event, ComplexEvent)
    assert event.val == 123

def test_hydrate_unknown_type():
    stored = MockStoredEvent(event_type="Unknown", payload={})
    assert EventTypeRegistry.hydrate(stored) is None

def test_hydrate_type_error():
    # Payload missing argument
    EventTypeRegistry.register_class(SimpleEvent)
    stored = MockStoredEvent(event_type="SimpleEvent", payload={}) # missing 'data'
    
    assert EventTypeRegistry.hydrate(stored) is None

def test_hydrate_dict():
    EventTypeRegistry.register_class(SimpleEvent)
    event = EventTypeRegistry.hydrate_dict("SimpleEvent", {"data": "foo"})
    assert isinstance(event, SimpleEvent)
    assert event.data == "foo"

def test_hydrate_dict_unknown():
    assert EventTypeRegistry.hydrate_dict("Unknown", {}) is None

def test_hydrate_dict_error():
    EventTypeRegistry.register_class(SimpleEvent)
    assert EventTypeRegistry.hydrate_dict("SimpleEvent", {}) is None # Missing data

def test_hydrate_dict_with_from_dict():
    EventTypeRegistry.register_class(ComplexEvent)
    event = EventTypeRegistry.hydrate_dict("ComplexEvent", {"val": "999"})
    assert isinstance(event, ComplexEvent)
    assert event.val == 999
