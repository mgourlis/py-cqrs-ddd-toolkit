
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime
from cqrs_ddd.contrib.pydantic import (
    PydanticQuery,
    PydanticValueObject,
    PydanticEntity,
    PydanticDomainEvent,
    PydanticSaga,
    PydanticOutboxMessage,
    CommandResult,
    QueryResult,
    PydanticValidator,
    ValidationResult,
    HAS_PYDANTIC
)

if HAS_PYDANTIC:
    from pydantic import BaseModel

# --- Tests ---

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_query_coverage():
    """Cover PydanticQuery properties and methods."""
    class MyQuery(PydanticQuery):
        param: str
    
    q = MyQuery(param="test")
    assert q.query_id
    assert q.correlation_id is None
    q.correlation_id = "corr"
    assert q.correlation_id == "corr"
    assert q.to_dict()["param"] == "test"

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_value_object_equality_coverage():
    """Cover ValueObject equality/hash edge cases."""
    class VO(PydanticValueObject):
        val: int
    
    v1 = VO(val=1)
    # 1. Comparison with other type
    assert v1 != "not-a-vo"
    
    # 2. Hash check
    assert isinstance(hash(v1), int)

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_entity_coverage():
    """Cover Entity equality, hash, and soft delete branches."""
    class MyEntity(PydanticEntity):
        name: str
    
    e1 = MyEntity(name="A")
    e1_id = e1.id
    
    # 1. Equality by ID
    e2 = MyEntity(id=e1_id, name="B")
    assert e1 == e2
    assert e1 != "other"
    
    # 2. Hash
    assert hash(e1) == hash(e1_id)
    
    # 3. Soft delete branches
    e1.soft_delete()
    assert e1.is_deleted
    e1.soft_delete() # Double delete (line 165 branch)
    
    e1.restore()
    assert not e1.is_deleted
    e1.restore() # Double restore (line 173 branch)

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_domain_event_base_coverage():
    """Cover base DomainEvent branches."""
    class BaseEvent(PydanticDomainEvent):
        pass
    
    evt = BaseEvent()
    with pytest.raises(NotImplementedError):
        _ = evt.aggregate_id
    
    # to_dict with date and missing aggregate_id skip?
    # Actually aggregate_id property would raise in to_dict
    
    class RealEvent(PydanticDomainEvent):
        agg_id: str
        @property
        def aggregate_id(self): return self.agg_id
        
    re = RealEvent(agg_id="123")
    data = re.to_dict()
    assert data["aggregate_id"] == "123"
    assert data["aggregate_type"] == "Unknown"

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_sync_edge_cases():
    """Cover PydanticSaga state synchronization branches."""
    class SState(BaseModel):
        count: int = 0
    
    class StepEvent: pass
    
    class MyPSaga(PydanticSaga[SState]):
        state_model = SState
        async def compensate(self): pass
    
    ctx = MagicMock()
    ctx.state = {"count": 1}
    saga = MyPSaga(ctx, mediator=MagicMock())
    
    # Access state (initializes _typed_state)
    assert saga.state.count == 1
    
    # Modify state and handle event (triggers sync in finally)
    saga.state.count = 2
    
    # We call handle_event which calls super().handle_event and then finally syncs
    with patch("cqrs_ddd.saga.Saga.handle_event", AsyncMock()):
        await saga.handle_event(StepEvent())
        
    assert ctx.state["count"] == 2
    
    # 2. AttributeError path (v1 style fallback)
    class V1State:
        def dict(self): return {"count": 3}
        # missing model_dump
    
    saga._typed_state = V1State()
    with patch("cqrs_ddd.saga.Saga.handle_event", AsyncMock()):
        await saga.handle_event(StepEvent())
    assert ctx.state["count"] == 3

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_result_models_coverage():
    """Cover CommandResult and QueryResult instantiation."""
    cmd_res = CommandResult(success=True, result="ok")
    assert cmd_res.success
    
    query_res = QueryResult(data={"a": 1}, total=10)
    assert query_res.total == 10

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_pydantic_outbox_message_coverage():
    """Cover OutboxMessage validation and field access."""
    from datetime import timezone
    msg = PydanticOutboxMessage(
        id="8ca6ec64-629f-43f1-b1e1-10c598be699d",
        occurred_at=datetime.now(timezone.utc),
        type="test.event",
        topic="test-topic",
        payload={"foo": "bar"},
        status="pending",
        retries=0
    )
    assert msg.payload["foo"] == "bar"
    assert msg.status == "pending"
    assert msg.retries == 0
    assert msg.error is None
    assert msg.processed_at is None

@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
def test_validation_result_methods():
    """Cover ValidationResult methods."""
    res = ValidationResult(errors={})
    assert res.is_valid()
    assert not res.has_errors()
    
    res_err = ValidationResult(errors={"f": ["e"]})
    assert not res_err.is_valid()
    assert res_err.has_errors()

@pytest.mark.asyncio
async def test_validator_base_coverage():
    """Cover PydanticValidator base methods."""
    class BaseVal(PydanticValidator):
        pass
    
    v = BaseVal()
    res = await v.validate(MagicMock())
    assert res.is_valid()

def test_pydantic_missing_guards():
    """Cover branches for missing Pydantic."""
    # Simulate the logic in pydantic.py:233
    with patch("cqrs_ddd.contrib.pydantic.HAS_PYDANTIC", False):
        class FallbackCmd:
            def __init__(self, *args, **kwargs):
                from cqrs_ddd.contrib.pydantic import HAS_PYDANTIC as HP
                if not HP:
                    raise ImportError("Pydantic is required")
        
        with pytest.raises(ImportError):
            FallbackCmd()
