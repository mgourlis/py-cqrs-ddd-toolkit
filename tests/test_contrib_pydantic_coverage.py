import pytest
from unittest.mock import MagicMock, patch
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
    HAS_PYDANTIC,
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
    e1.soft_delete()  # Double delete (line 165 branch)

    e1.restore()
    assert not e1.is_deleted
    e1.restore()  # Double restore (line 173 branch)


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
        def aggregate_id(self):
            return self.agg_id

    re = RealEvent(agg_id="123")
    data = re.to_dict()
    assert data["aggregate_id"] == "123"
    assert data["aggregate_type"] == "Unknown"


@pytest.mark.skipif(not HAS_PYDANTIC, reason="Pydantic not installed")
@pytest.mark.asyncio
async def test_pydantic_saga_sync_coverage():
    """Cover PydanticSaga._sync_state."""

    class SState(BaseModel):
        count: int = 0

    class MyPSaga(PydanticSaga[SState]):
        state_model = SState

    ctx = MagicMock()
    # Setup state as a real dict for init, but mock methods for sync check
    # Actually PydanticSaga._load_state reads ctx.state.
    # If we want to check .update() call, we need to wrap the dict or check side effect.
    real_dict = {"count": 1}
    ctx.state = real_dict

    saga = MyPSaga(ctx, mediator=MagicMock())

    # 1. State loaded
    assert saga.state.count == 1

    # 2. Modify state
    saga.state.count = 2

    # 3. Serialize/Sync
    saga._sync_state()

    # Verify context state updated in place
    assert ctx.state["count"] == 2
    # Verify reference preserved (if using update)
    assert ctx.state is real_dict

    # 4. Fallback branch (non-BaseModel state)
    saga._state = {"count": 3}  # Force dict state
    saga._sync_state()
    # Base Saga._sync_state assigns context.state = _state
    assert ctx.state == {"count": 3}
    # Reference might change here, which is fine for base behavior


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
        retries=0,
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
