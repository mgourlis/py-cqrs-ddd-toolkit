import pytest
from unittest.mock import MagicMock, AsyncMock
import uuid
import sys

# Check if pydantic is installed, otherwise skip
try:
    from pydantic import ValidationError, BaseModel
    from cqrs_ddd.contrib.pydantic import (
        PydanticCommand, 
        PydanticQuery, 
        PydanticEntity, 
        PydanticValueObject,
        PydanticDomainEvent,
        PydanticSaga,
        PydanticValidator,
        ValidationResult
    )
    from cqrs_ddd.saga import saga_step
    from cqrs_ddd.saga_registry import saga_registry
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False

@pytest.fixture(autouse=True)
def clean_saga_registry():
    if "saga_registry" in globals() or "cqrs_ddd.saga_registry" in sys.modules:
        from cqrs_ddd.saga_registry import saga_registry
        saga_registry.clear()
    yield
    if "saga_registry" in globals() or "cqrs_ddd.saga_registry" in sys.modules:
        from cqrs_ddd.saga_registry import saga_registry
        saga_registry.clear()

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_command():
    class CreateUser(PydanticCommand):
        name: str
    
    cmd = CreateUser(name="Alice")
    assert cmd.name == "Alice"
    assert cmd.command_id
    assert cmd.correlation_id is None
    
    cmd.correlation_id = "abc"
    assert cmd.correlation_id == "abc"
    
    assert cmd.to_dict()["name"] == "Alice"

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_value_object():
    class Money(PydanticValueObject):
        amount: int
        currency: str
        
    m1 = Money(amount=100, currency="USD")
    m2 = Money(amount=100, currency="USD")
    m3 = Money(amount=50, currency="EUR")
    
    assert m1 == m2
    assert m1 != m3
    assert hash(m1) == hash(m2)
    
    # Immutability check (frozen)
    with pytest.raises(ValidationError):
        m1.amount = 200

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_entity():
    class User(PydanticEntity):
        name: str
        
    u = User(name="Bob")
    assert u.id
    assert u.version == 0
    assert u.created_at
    
    # Version increment
    u.increment_version()
    assert u.version == 1
    assert u.updated_at
    
    # Soft delete
    u.soft_delete()
    assert u.is_deleted
    assert u.deleted_at
    assert u.version == 2
    
    # Restore
    u.restore()
    assert not u.is_deleted
    assert u.deleted_at is None
    assert u.version == 3

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_domain_event():
    # Subclassing triggers auto-registration, mock registry to avoid side effects
    class UserCreated(PydanticDomainEvent):
        user_id: str
        
        @property
        def aggregate_id(self):
            return self.user_id

    evt = UserCreated(user_id="u1")
    assert evt.event_type == "UserCreated"
    assert evt.aggregate_id == "u1"
    
    data = evt.to_dict()
    assert data["event_type"] == "UserCreated"
    assert data["user_id"] == "u1"

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga():
    from cqrs_ddd.saga import SagaContext
    
    class MyEvent: pass
    
    class MyState(BaseModel):
        count: int = 0
        
    class MySaga(PydanticSaga[MyState]):
        state_model = MyState
        
        @saga_step(MyEvent)
        async def on_MyEvent(self, event):
            self.state.count += 1
            
        async def compensate(self):
            pass
            
    # Use REAL SagaContext
    state_dict = {"count": 10}
    context = SagaContext(
        saga_id="s1",
        saga_type="MySaga",
        correlation_id="c1",
        current_step="step1",
        state=state_dict
    )
    
    # Mock mediator
    mediator = MagicMock()
    
    saga = MySaga(context, mediator=mediator)
    
    # Run handler
    event = MyEvent()
    await saga.handle_event(event)
    
    # Verify handler was reached (history update)
    assert len(saga.context.history) > 0
    assert saga.context.history[0]["action"] == "handle_event"
    
    # Verify handler ran
    assert saga.state.count == 11
    
    # Sync state manually for unit test
    saga._sync_state()
    
    # Verify state update synced to context
    assert saga.context.state["count"] == 11
    assert state_dict["count"] == 11

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_validator():
    class MyValidator(PydanticValidator):
        async def init_context(self, cmd):
            self.context_set = True
            
        def validate_foo(self, val):
            if val == "bad":
                raise ValueError("Is bad")
                
    v = MyValidator()
    cmd = MagicMock()
    cmd.foo = "good"
    
    res = await v.validate(cmd)
    assert res.is_valid()
    assert v.context_set
    
    cmd.foo = "bad"
    res = await v.validate(cmd)
    assert not res.is_valid()
    assert "Is bad" in res.errors["foo"]
