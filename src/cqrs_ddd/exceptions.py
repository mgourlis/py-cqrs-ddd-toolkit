"""Exceptions for CQRS/DDD toolkit."""
from typing import Dict, Any, List


class CQRSDDDError(Exception):
    """Base exception for all CQRS/DDD toolkit errors."""
    pass


class ValidationError(CQRSDDDError):
    """Raised when command/query validation fails."""
    
    def __init__(self, message_type: str, errors: Dict[str, List[str]]):
        """
        Initialize validation error.
        
        Args:
            message_type: The type of message (Command/Query/Event) that failed.
            errors: Dictionary of validation errors {field: [messages]}.
        """
        self.message_type = message_type
        self.errors = errors
        super().__init__(f"Validation failed for {message_type}: {errors}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "error": "validation_error",
            "message_type": self.message_type,
            "errors": self.errors,
        }


class EntityNotFoundError(CQRSDDDError):
    """Raised when an entity is not found."""
    
    def __init__(self, entity_type: str, entity_id: Any):
        """
        Initialize entity not found error.
        
        Args:
            entity_type: The name/type of the entity.
            entity_id: The ID of the requested entity.
        """
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} with ID '{entity_id}' not found")


class HandlerNotFoundError(CQRSDDDError):
    """Raised when no handler is registered for a message type."""
    
    def __init__(self, message_type: type):
        self.message_type = message_type
        super().__init__(f"No handler registered for {message_type.__name__}")


class DomainError(CQRSDDDError):
    """Raised when a domain rule is violated."""
    
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"[{code}] {message}")


class InfrastructureError(CQRSDDDError):
    """Raised when an infrastructure operation fails."""
    
    def __init__(self, code: str, message: str, original_error: Exception = None):
        self.code = code
        self.message = message
        self.original_error = original_error
        super().__init__(f"[{code}] {message}")


class AuthorizationError(CQRSDDDError):
    """Raised when permission check fails."""
    
    def __init__(self, message: str = "Access denied"):
        super().__init__(message)


class ConcurrencyError(CQRSDDDError):
    """Raised when optimistic concurrency check fails."""
    
    def __init__(self, expected: int, actual: int):
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Concurrency error: expected version {expected}, but found {actual}"
        )
