import pytest
import uuid
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from cqrs_ddd.ddd import (
    Entity,
    AggregateRoot,
    ValueObject,
    Modification,
    default_id_generator,
)
from cqrs_ddd.exceptions import ConcurrencyError


# --- Test Entity ---

class User(Entity):
    def __init__(self, username: str, **kwargs):
        super().__init__(**kwargs)
        self.username = username

def test_default_id_generator():
    id1 = default_id_generator()
    id2 = default_id_generator()
    assert isinstance(id1, str)
    assert uuid.UUID(id1)  # validating it's a UUID
    assert id1 != id2

def test_entity_initialization():
    user = User(username="alice")
    
    assert user.id is not None
    assert user.version == 0
    assert isinstance(user.created_at, datetime)
    assert user.updated_at is None
    assert user.created_by is None
    assert user.is_deleted is False
    assert user.deleted_at is None
    assert user.username == "alice"

def test_entity_initialization_with_values():
    now = datetime.now()
    user = User(
        username="bob",
        entity_id="user-123",
        version=5,
        created_at=now,
        created_by="admin",
        is_deleted=True,
        deleted_at=now
    )
    
    assert user.id == "user-123"
    assert user.version == 5
    assert user.created_at == now
    assert user.created_by == "admin"
    assert user.is_deleted is True
    assert user.deleted_at == now

def test_entity_increment_version():
    user = User(username="charlie")
    assert user.version == 0
    assert user.updated_at is None
    
    user.increment_version()
    
    assert user.version == 1
    assert user.updated_at is not None
    assert isinstance(user.updated_at, datetime)

def test_entity_check_version_success():
    user = User(username="dave", version=2)
    # Should not raise
    user.check_version(2)

def test_entity_check_version_failure():
    user = User(username="eve", version=2)
    
    with pytest.raises(ConcurrencyError) as excinfo:
        user.check_version(3)
    
    assert excinfo.value.expected == 3
    assert excinfo.value.actual == 2

def test_entity_soft_delete():
    user = User(username="frank")
    assert not user.is_deleted
    
    user.soft_delete()
    
    assert user.is_deleted
    assert user.deleted_at is not None
    assert user.version == 1

def test_entity_soft_delete_idempotent():
    user = User(username="frank")
    user.soft_delete()
    initial_version = user.version
    deleted_at = user.deleted_at
    
    # Second call should do nothing
    user.soft_delete()
    
    assert user.is_deleted
    assert user.deleted_at == deleted_at
    assert user.version == initial_version

def test_entity_restore():
    user = User(username="grace", is_deleted=True, deleted_at=datetime.now())
    
    user.restore()
    
    assert not user.is_deleted
    assert user.deleted_at is None
    assert user.version > 0

def test_entity_restore_idempotent():
    user = User(username="grace")
    assert not user.is_deleted
    
    # Should do nothing
    user.restore()
    
    assert not user.is_deleted
    assert user.version == 0

def test_entity_equality():
    u1 = User(entity_id="1", username="same")
    u2 = User(entity_id="1", username="same")
    u3 = User(entity_id="2", username="diff")
    
    assert u1 == u2
    assert u1 != u3
    assert u1 != "string"  # Type check

def test_entity_hash():
    u1 = User(entity_id="1", username="same")
    u2 = User(entity_id="1", username="same")
    
    assert hash(u1) == hash(u2)
    assert isinstance(hash(u1), int)

# --- Test AggregateRoot ---

class Order(AggregateRoot):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

def test_aggregate_root_events():
    order = Order()
    assert order.version == 0
    assert len(order.clear_domain_events()) == 0
    
    # Adding event should increment version
    order.add_domain_event("OrderCreated")
    
    assert order.version == 1
    events = order.clear_domain_events()
    assert events == ["OrderCreated"]
    
    # Events should be cleared
    assert len(order.clear_domain_events()) == 0

# --- Test ValueObject ---

@dataclass(frozen=True)
class Address(ValueObject):
    street: str
    city: str

def test_value_object_equality():
    a1 = Address(street="Main St", city="City")
    a2 = Address(street="Main St", city="City")
    a3 = Address(street="Other St", city="City")
    
    assert a1 == a2
    assert a1 != a3
    assert a1 != "string"

def test_value_object_hash():
    a1 = Address(street="Main St", city="City")
    a2 = Address(street="Main St", city="City")
    
    assert hash(a1) == hash(a2)

# --- Test Modification ---

@pytest.mark.asyncio
async def test_modification():
    user = User(username="harry")
    mod = Modification(entity=user, events=["e1", "e2"])
    
    assert mod.entity == user
    assert mod.events == ["e1", "e2"]
    assert mod.entity_name == "User"
    assert mod.get_domain_events() == ["e1", "e2"]
    
    # Ensure get_domain_events returns a copy
    events = mod.get_domain_events()
    events.append("e3")
    assert mod.events == ["e1", "e2"]

@pytest.mark.asyncio
async def test_modification_apply():
    user = User(username="harry")
    mod = Modification(entity=user)
    
    mock_dispatcher = MagicMock()
    mock_dispatcher.apply = AsyncMock(return_value="result")
    
    res = await mod.apply(dispatcher=mock_dispatcher, unit_of_work="uow")
    
    assert res == "result"
    mock_dispatcher.apply.assert_awaited_once_with(mod, "uow")
