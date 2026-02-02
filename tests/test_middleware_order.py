import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import List

from cqrs_ddd.mediator import Mediator
from cqrs_ddd.core import Command, CommandResponse, CommandHandler


@dataclass
class OrderCommand(Command):
    data: str = "test"


class MiddlewareTracker:
    def __init__(self):
        self.order: List[str] = []

    def record(self, tag: str):
        self.order.append(tag)


class TrackingMiddleware:
    def __init__(self, name: str, tracker: MiddlewareTracker):
        self.name = name
        self.tracker = tracker

    def apply(self, handler, message):
        async def wrapper(msg):
            self.tracker.record(f"{self.name}_entry")
            result = await handler(msg)
            self.tracker.record(f"{self.name}_exit")
            return result

        return wrapper


@pytest.mark.asyncio
async def test_middleware_execution_order_onion_model():
    """
    Verifies that middlewares are executed in the correct 'onion' order.
    Top-most decorator should be the outermost layer.
    """
    tracker = MiddlewareTracker()
    mediator = Mediator()

    # We'll simulate 3 middlewares: A (outer), B (middle), C (inner)
    mw_a = TrackingMiddleware("A", tracker)
    mw_b = TrackingMiddleware("B", tracker)
    mw_c = TrackingMiddleware("C", tracker)

    class OrderHandler(CommandHandler):
        # We manually set _middlewares to simulate the registry
        # The registry appends them in reverse order of application (bottom-up in Python)
        # So [C, B, A] corresponds to:
        # @A
        # @B
        # @C
        # class OrderHandler...
        _middlewares = [mw_c, mw_b, mw_a]

        async def handle(self, cmd: OrderCommand) -> CommandResponse:
            tracker.record("handler_exec")
            return CommandResponse(result="ok")

    mediator.register(OrderCommand, OrderHandler())

    await mediator.send(OrderCommand())

    # Expected Flow:
    # A_entry -> B_entry -> C_entry -> handler -> C_exit -> B_exit -> A_exit
    expected_order = [
        "A_entry",
        "B_entry",
        "C_entry",
        "handler_exec",
        "C_exit",
        "B_exit",
        "A_exit",
    ]

    assert tracker.order == expected_order


@pytest.mark.asyncio
async def test_middleware_execution_order_with_registry():
    """
    Verifies that the @middleware decorators result in the correct onion order.
    """
    tracker = MiddlewareTracker()

    # Create custom middleware classes for testing
    class CustomMW:
        def __init__(self, name: str):
            self.name = name

        def apply(self, handler, msg):
            async def wrapper(m):
                tracker.record(f"{self.name}_entry")
                res = await handler(m)
                tracker.record(f"{self.name}_exit")
                return res

            return wrapper

    # Register them dynamically in the registry for this test
    # (Mocking the registry behavior)
    with patch(
        "cqrs_ddd.middleware.MiddlewareRegistry.log",
        side_effect=lambda: lambda cls: cls,
    ):  # just bypass
        # We'll manually construct the _middlewares list to see what mediator does
        # because the real registry uses MiddlewareDefinition which instantiates classes

        mediator = Mediator()

        # Let's say we have a handler with actual definitions
        from cqrs_ddd.middleware import MiddlewareDefinition

        class TestHandler(CommandHandler):
            _middlewares = [
                MiddlewareDefinition(lambda: CustomMW("INNER")),
                MiddlewareDefinition(lambda: CustomMW("MIDDLE")),
                MiddlewareDefinition(lambda: CustomMW("OUTER")),
            ]

            async def handle(self, cmd: OrderCommand):
                tracker.record("handler")
                return CommandResponse(result="ok")

        mediator.register(OrderCommand, TestHandler())
        await mediator.send(OrderCommand())

        # Mediator processes list from start to end.
        # So [INNER, MIDDLE, OUTER] means OUTER is applied last, wrapping MIDDLE, wrapping INNER.
        # Thus OUTER entry happens first.

        expected = [
            "OUTER_entry",
            "MIDDLE_entry",
            "INNER_entry",
            "handler",
            "INNER_exit",
            "MIDDLE_exit",
            "OUTER_exit",
        ]
        assert tracker.order == expected


@pytest.mark.asyncio
async def test_validator_middleware_order_and_fail_fast():
    """
    Verifies that ValidatorMiddleware correctly blocks execution when validation fails,
    and that outer middlewares (like Logging) still run their post-processing/error handling.
    """
    from cqrs_ddd.middleware import (
        LoggingMiddleware,
        ValidatorMiddleware,
        ThreadSafetyMiddleware,
    )
    from cqrs_ddd.validation import ValidationResult
    from cqrs_ddd.exceptions import ValidationError

    tracker = MiddlewareTracker()
    mediator = Mediator()

    # Custom Mock Validator to record its execution
    class MyFailingValidator:
        async def validate(self, msg):
            tracker.record("validator_run")
            return ValidationResult.failure({"field": ["error"]})

    # Mock Lock Strategy to record its execution
    mock_lock = MagicMock()
    mock_lock.acquire = AsyncMock(
        side_effect=lambda *a, **k: tracker.record("lock_acquired") or "token"
    )
    mock_lock.release = AsyncMock(
        side_effect=lambda *a, **k: tracker.record("lock_released")
    )

    # We'll use patched logging for the real LoggingMiddleware to record its execution via tracker
    with patch("logging.getLogger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        # Record logger calls to tracker
        mock_logger.info.side_effect = lambda msg: tracker.record("log_info")
        mock_logger.error.side_effect = lambda msg: tracker.record("log_error")

        class ValidationOrderHandler(CommandHandler):
            # Simulation of:
            # @middleware.log()
            # @middleware.validate(MyFailingValidator)
            # @middleware.thread_safe(entity_type="T", entity_id_attr="data", lock_strategy=mock_lock)
            _middlewares = [
                ThreadSafetyMiddleware("T", "data", lock_strategy=mock_lock),
                ValidatorMiddleware(MyFailingValidator),
                LoggingMiddleware(),
            ]

            async def handle(self, cmd: OrderCommand):
                tracker.record("handler_exec")
                return CommandResponse(result="ok")

        mediator.register(OrderCommand, ValidationOrderHandler())

        # Execution should fail with ValidationError
        with pytest.raises(ValidationError):
            await mediator.send(OrderCommand())

        # Expected sequence (Top-to-Bottom application):
        # 1. LoggingMiddleware entries
        # 2. ValidatorMiddleware runs and FAILS
        # -> ThreadSafetyMiddleware is NEVER reached (no lock acquired)
        # -> Handler is NEVER reached
        # 3. LoggingMiddleware catches error and records it

        expected_sequence = [
            "log_info",  # 1. Logging entry
            "validator_run",  # 2. Validator runs
            "log_error",  # 3. Logging catches failure
        ]

        assert tracker.order == expected_sequence
        # Verify lock was NOT acquired
        mock_lock.acquire.assert_not_called()
        # Verify handler was NOT called
        assert "handler_exec" not in tracker.order


@pytest.mark.asyncio
async def test_validator_success_full_flow():
    """
    Verifies that when validation succeeds, everything runs in the correct sequence.
    """
    from cqrs_ddd.middleware import (
        LoggingMiddleware,
        ValidatorMiddleware,
        ThreadSafetyMiddleware,
    )
    from cqrs_ddd.validation import ValidationResult

    tracker = MiddlewareTracker()
    mediator = Mediator()

    class MyPassingValidator:
        async def validate(self, msg):
            tracker.record("validator_run")
            return ValidationResult.success()

    mock_lock = MagicMock()
    mock_lock.acquire = AsyncMock(
        side_effect=lambda *a, **k: tracker.record("lock_acquired") or "token"
    )
    mock_lock.release = AsyncMock(
        side_effect=lambda *a, **k: tracker.record("lock_released")
    )

    with patch("logging.getLogger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_logger.info.side_effect = lambda msg: tracker.record("log_info")

        class ValidationSuccessHandler(CommandHandler):
            _middlewares = [
                ThreadSafetyMiddleware("T", "data", lock_strategy=mock_lock),
                ValidatorMiddleware(MyPassingValidator),
                LoggingMiddleware(),
            ]

            async def handle(self, cmd: OrderCommand):
                tracker.record("handler_exec")
                return CommandResponse(result="ok")

        mediator.register(OrderCommand, ValidationSuccessHandler())
        await mediator.send(OrderCommand())

        # Expected Sequence:
        # Logging entry -> Validator -> ThreadSafe Entry -> Handler -> ThreadSafe Exit -> Logging Exit
        expected = [
            "log_info",  # Logging Entry
            "validator_run",  # Validator Entry
            "lock_acquired",  # Thread Safe Entry
            "handler_exec",  # Handler
            "lock_released",  # Thread Safe Exit
            "log_info",  # Logging Exit
        ]

        assert tracker.order == expected
