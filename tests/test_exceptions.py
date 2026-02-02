from cqrs_ddd.exceptions import (
    CQRSDDDError,
    ValidationError,
    EntityNotFoundError,
    HandlerNotFoundError,
    DomainError,
    InfrastructureError,
    AuthorizationError,
    ConcurrencyError,
)


def test_base_exception():
    exc = CQRSDDDError("base error")
    assert str(exc) == "base error"


def test_validation_error():
    errors = {"field": ["required"]}
    exc = ValidationError("MyCommand", errors)
    assert "Validation failed for MyCommand" in str(exc)

    data = exc.to_dict()
    assert data["error"] == "validation_error"
    assert data["message_type"] == "MyCommand"
    assert data["errors"] == errors


def test_entity_not_found_error():
    exc = EntityNotFoundError("User", 123)
    assert str(exc) == "User with ID '123' not found"
    assert exc.entity_type == "User"
    assert exc.entity_id == 123


def test_handler_not_found_error():
    class MyMsg:
        pass

    exc = HandlerNotFoundError(MyMsg)
    assert str(exc) == "No handler registered for MyMsg"
    assert exc.message_type == MyMsg


def test_domain_error():
    exc = DomainError("INVALID_STATE", "State is wrong")
    assert str(exc) == "[INVALID_STATE] State is wrong"
    assert exc.code == "INVALID_STATE"


def test_infrastructure_error():
    orig = ValueError("boom")
    exc = InfrastructureError("DB_FAIL", "Database unavailable", orig)
    assert str(exc) == "[DB_FAIL] Database unavailable"
    assert exc.original_error == orig


def test_authorization_error():
    exc = AuthorizationError("Forbidden")
    assert str(exc) == "Forbidden"

    exc_def = AuthorizationError()
    assert str(exc_def) == "Access denied"


def test_concurrency_error():
    exc = ConcurrencyError(expected=2, actual=1)
    assert "expected version 2, but found 1" in str(exc)
