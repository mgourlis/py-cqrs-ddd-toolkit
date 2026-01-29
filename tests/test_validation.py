import pytest
from unittest.mock import AsyncMock, MagicMock
from cqrs_ddd.validation import (
    ValidationResult, 
    CompositeValidator, 
    PydanticValidator,
    Validator
)

# --- Tests ---

def test_validation_result_logic():
    res = ValidationResult.success()
    assert not res.has_errors()
    assert res.is_valid()
    assert res.errors == {}
    
    res_fail = ValidationResult.failure({"f": ["err"]})
    assert res_fail.has_errors()
    assert not res_fail.is_valid()
    assert res_fail.errors["f"] == ["err"]

@pytest.mark.asyncio
async def test_composite_validator():
    # Mock validators
    v1 = MagicMock()
    v1.validate = AsyncMock(return_value=ValidationResult.failure({"f1": ["e1"]}))
    
    v2 = MagicMock()
    v2.validate = AsyncMock(return_value=ValidationResult.failure({"f2": ["e2"]}))
    
    v3 = MagicMock()
    v3.validate = AsyncMock(return_value=ValidationResult.success())
    
    composite = CompositeValidator([v1, v2, v3])
    msg = MagicMock()
    
    res = await composite.validate(msg)
    
    assert res.has_errors()
    assert "f1" in res.errors
    assert "f2" in res.errors
    assert res.errors["f1"] == ["e1"]
    assert res.errors["f2"] == ["e2"]

@pytest.mark.asyncio
async def test_pydantic_validator_flow():
    class TestValidator(PydanticValidator):
        def __init__(self):
            super().__init__()
            self.context_loaded = False
            
        async def init_context(self, message):
            self.context_loaded = True
            
        def validate_sync_field(self, value):
            if value == "bad":
                raise ValueError("Must not be bad")
                
        async def validate_async_field(self, value):
            if value < 0:
                raise ValueError("Must be positive")
                
        def validate_broken(self, value):
             if value == "crash":
                 raise RuntimeError("Oops")
    
    validator = TestValidator()
    
    # Case 1: Valid
    msg = MagicMock()
    msg.sync_field = "good"
    msg.async_field = 10
    msg.broken = "safe"
    
    res = await validator.validate(msg)
    assert not res.has_errors()
    assert validator.context_loaded
    
    # Case 2: Errors
    msg2 = MagicMock()
    msg2.sync_field = "bad"
    msg2.async_field = -5
    msg2.broken = "crash"
    
    res2 = await validator.validate(msg2)
    assert res2.has_errors()
    assert "sync_field" in res2.errors
    assert "async_field" in res2.errors
    assert "broken" in res2.errors
    
    assert "Must not be bad" in res2.errors["sync_field"][0]
    assert "Must be positive" in res2.errors["async_field"][0]
    assert "Validation logic error: Oops" in res2.errors["broken"][0]
    
    # Case 3: Missing field on message -> treated as None passed to validator func
    msg3 = MagicMock(spec=[]) # No attributes
    # The validator getattr returns None
    # function receives None
    
    # Let's adjust mock validator to fail on None
    class NoneCheckValidator(PydanticValidator):
        def validate_required(self, value):
            if value is None:
                raise ValueError("Required")
                
    v3 = NoneCheckValidator()
    res3 = await v3.validate(msg3)
    assert "required" in res3.errors
    assert "Required" in res3.errors["required"][0]
