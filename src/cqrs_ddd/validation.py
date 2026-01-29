"""Validation machinery."""
from typing import Dict, List, Any, TypeVar, Generic, Type, Optional, Callable
from dataclasses import dataclass
from .protocols import Validator

T = TypeVar('T')

try:
    from pydantic import BaseModel, Field, ValidationError
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = object
    class ValidationError(Exception): pass


@dataclass
class ValidationResult:
    """Standard validation result."""
    
    errors: Dict[str, List[str]]
    
    def has_errors(self) -> bool:
        """Check if there are any validation errors."""
        return len(self.errors) > 0
    
    def is_valid(self) -> bool:
        """Check if validation passed."""
        return not self.has_errors()
    
    @classmethod
    def success(cls) -> 'ValidationResult':
        """Create a successful validation result (no errors)."""
        return cls(errors={})
        
    @classmethod
    def failure(cls, errors: Dict[str, List[str]]) -> 'ValidationResult':
        """
        Create a failed validation result.
        
        Args:
            errors: Dictionary mapping field names to lists of error messages.
        """
        return cls(errors=errors)


class CompositeValidator(Validator[T]):
    """Runs multiple validators in sequence."""
    
    def __init__(self, validators: List[Validator[T]]):
        self.validators = validators
        
    async def validate(self, message: T) -> ValidationResult:
        all_errors = {}
        
        for validator in self.validators:
            result = await validator.validate(message)
            if result.has_errors():
                for field, msgs in result.errors.items():
                    if field not in all_errors:
                        all_errors[field] = []
                    all_errors[field].extend(msgs)
                    
        return ValidationResult(errors=all_errors)


# =============================================================================
# Pydantic Support
# =============================================================================

class PydanticValidator(Validator[T]):
    """
    Base class for validators that leverage Pydantic.
    
    If the Command itself is a Pydantic model, it might already be validated 
    during instantiation by Pydantic. This helper adds async validation capability 
    (e.g., database checks) while providing a familiar API.
    """
    
    def __init__(self):
        self._errors: Dict[str, List[str]] = {}
        
    async def validate(self, message: T) -> ValidationResult:
        """Run validation logic."""
        self._errors = {}
        
        # 1. Init context (async lookups)
        await self.init_context(message)
        
        # 2. Run validate_ methods
        for name in dir(self):
            if name.startswith('validate_') and callable(getattr(self, name)):
                field_name = name[9:]
                validator_func = getattr(self, name)
                
                # Check if field exists on message
                value = getattr(message, field_name, None)
                
                try:
                    # Validator can be async
                    if self._is_async(validator_func):
                        await validator_func(value)
                    else:
                        validator_func(value)
                except ValueError as e:
                    self._add_error(field_name, str(e))
                except Exception as e:
                    self._add_error(field_name, f"Validation logic error: {e}")
                    
        return ValidationResult(errors=self._errors)
    
    async def init_context(self, message: T) -> None:
        """Override to load data needed for validation."""
        pass
        
    def _add_error(self, field: str, message: str):
        if field not in self._errors:
            self._errors[field] = []
        self._errors[field].append(message)
        
    def _is_async(self, func) -> bool:
        import inspect
        return inspect.iscoroutinefunction(func)
