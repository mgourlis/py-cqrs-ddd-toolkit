import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

from cqrs_ddd.middleware import (
    ValidatorMiddleware,
    LoggingMiddleware,
    ThreadSafetyMiddleware,
    AttributeInjectionMiddleware,
    EventStorePersistenceMiddleware,
    MiddlewareRegistry,
)
from cqrs_ddd.core import CommandResponse
from cqrs_ddd.exceptions import ValidationError

# --- Helper Objects ---


@dataclass
class SimpleCommand:
    name: str = "test"
    command_id: str = "cmd-123"
    correlation_id: str = None


@dataclass
class CommandResult:
    events: list = None
    correlation_id: str = None
    causation_id: str = None


# --- Validator Middleware Tests ---


@pytest.mark.asyncio
async def test_validator_middleware_success_new_protocol():
    mock_validator_instance = MagicMock()
    # Mock result object with has_errors() returning False
    mock_result = MagicMock()
    mock_result.has_errors.return_value = False
    mock_validator_instance.validate = AsyncMock(return_value=mock_result)

    mock_validator_class = MagicMock(return_value=mock_validator_instance)

    middleware = ValidatorMiddleware(validator_class=mock_validator_class)

    command = SimpleCommand()
    handler = AsyncMock(return_value=CommandResponse(result="success"))

    wrapped = middleware.apply(handler, command)
    result = await wrapped()

    assert result == CommandResponse(result="success")
    mock_validator_class.assert_called_once()
    mock_validator_instance.validate.assert_awaited_once_with(command)


@pytest.mark.asyncio
async def test_validator_middleware_failure():
    mock_validator_instance = MagicMock()
    mock_result = MagicMock()
    mock_result.has_errors.return_value = True
    mock_result.errors = {"field": ["error"]}
    mock_validator_instance.validate = AsyncMock(return_value=mock_result)

    mock_validator_class = MagicMock(return_value=mock_validator_instance)

    middleware = ValidatorMiddleware(validator_class=mock_validator_class)

    command = SimpleCommand()
    handler = AsyncMock()

    wrapped = middleware.apply(handler, command)

    with pytest.raises(ValidationError) as exc:
        await wrapped()

    # Check that error details are correct
    assert "SimpleCommand" in str(exc.value)


# --- Logging Middleware Tests ---


@pytest.mark.asyncio
async def test_logging_middleware_execution():
    middleware = LoggingMiddleware()
    command = SimpleCommand(correlation_id="corr-123")
    handler = AsyncMock(return_value=CommandResponse(result="done"))

    with patch("logging.getLogger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        wrapped = middleware.apply(handler, command)
        result = await wrapped()

        assert result == CommandResponse(result="done")

        # Verify info logs were called (start and end)
        assert mock_logger.info.call_count >= 2
        # Check that correlation ID is in the log message
        args, _ = mock_logger.info.call_args_list[0]
        assert "[corr-123]" in args[0]


@pytest.mark.asyncio
async def test_logging_middleware_error():
    middleware = LoggingMiddleware()
    command = SimpleCommand()
    handler = AsyncMock(side_effect=ValueError("boom"))

    with patch("logging.getLogger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        wrapped = middleware.apply(handler, command)

        with pytest.raises(ValueError):
            await wrapped()

        # Verify error log
        mock_logger.error.assert_called_once()


# --- Thread Safety Middleware Tests ---


@pytest.mark.asyncio
async def test_thread_safety_acquisition():
    mock_strategy = MagicMock()
    mock_strategy.acquire = AsyncMock(return_value="token")
    mock_strategy.release = AsyncMock()

    middleware = ThreadSafetyMiddleware(
        entity_type="User", entity_id_attr="name", lock_strategy=mock_strategy
    )

    command = SimpleCommand(name="alice")
    handler = AsyncMock(return_value=CommandResponse(result="ok"))

    wrapped = middleware.apply(handler, command)
    result = await wrapped()

    assert result == CommandResponse(result="ok")
    mock_strategy.acquire.assert_awaited_once_with("User", "alice")
    mock_strategy.release.assert_awaited_once_with("User", "alice", "token")


@pytest.mark.asyncio
async def test_thread_safety_missing_attr():
    middleware = ThreadSafetyMiddleware(
        entity_type="User", entity_id_attr="missing_field", lock_strategy=MagicMock()
    )
    command = SimpleCommand()
    handler = AsyncMock()

    wrapped = middleware.apply(handler, command)

    with pytest.raises(ValueError, match="must contain 'missing_field'"):
        await wrapped()


# --- Attribute Injection Middleware Tests ---


@pytest.mark.asyncio
async def test_attribute_injection():
    mock_injector = MagicMock()
    mock_injector.compute = AsyncMock(return_value=999)

    middleware = AttributeInjectionMiddleware(injectors={"user_id": mock_injector})

    command = SimpleCommand()
    handler = AsyncMock(return_value="ok")

    wrapped = middleware.apply(handler, command)
    await wrapped()

    assert getattr(command, "user_id") == 999
    mock_injector.compute.assert_awaited_once_with(command)


# --- Event Store Persistence Middleware Tests ---


@pytest.mark.asyncio
async def test_event_persistence_flow():
    mock_event_store = MagicMock()
    mock_event_store.append_batch = AsyncMock()

    middleware = EventStorePersistenceMiddleware(event_store=mock_event_store)

    command = SimpleCommand(command_id="cmd-1")
    # Simulate handler return a CommandResponse with events
    result_obj = CommandResponse(result="ok", events=["event1"])
    handler = AsyncMock(return_value=result_obj)

    # We also need to patch generate_correlation_id since it will be called if missing
    with patch(
        "cqrs_ddd.event_store.generate_correlation_id", return_value="gen-corr-id"
    ):
        # We also need to patch enrich_event_metadata to return something we can assert on
        with patch(
            "cqrs_ddd.domain_event.enrich_event_metadata", side_effect=lambda e, **k: e
        ):
            wrapped = middleware.apply(handler, command)
            result = await wrapped()

            # 1. Verify correlation ID was generated and set on command
            assert command.correlation_id == "gen-corr-id"

            # 2. Verify IDs propagated to result
            assert result.correlation_id == "gen-corr-id"
            assert result.causation_id == "cmd-1"

            # 3. Verify event store append called
            mock_event_store.append_batch.assert_awaited_once()
            args, kwargs = mock_event_store.append_batch.call_args
            assert args[0] == ["event1"]  # enirched events
            assert kwargs["correlation_id"] == "gen-corr-id"


# --- Middleware Registry Tests ---


def test_registry_decorators():
    registry = MiddlewareRegistry()

    @registry.log(level="debug")
    @registry.validate()
    class MyHandler:
        pass

    assert hasattr(MyHandler, "_middlewares")
    assert len(MyHandler._middlewares) == 2

    # Order of execution is reversed (inner decorator first in list usually, but let's check content)
    # Actually python decorators apply bottom-up, so validate is first applied, then log.
    # But usually decorators wrapper wraps the inner one.
    # Here the registry just appends to a list.

    # validate was applied first -> append
    # log was applied second -> append

    defs = MyHandler._middlewares
    assert defs[0].middleware_class == ValidatorMiddleware
    assert defs[1].middleware_class == LoggingMiddleware

    assert defs[1].kwargs == {"level": "debug"}
    assert defs[0].kwargs == {"validator_class": None}


def test_registry_convenience_decorators():
    from cqrs_ddd.middleware import AuthorizationMiddleware

    registry = MiddlewareRegistry()

    @registry.authorize(permissions=["read"])
    @registry.thread_safe(entity_type="T", entity_id_attr="id")
    @registry.inject_attrs(injectors={"u": 1})
    @registry.persist_events(event_store=MagicMock())
    class ComplexHandler:
        pass

    # Decorators apply bottom-up:
    # 1. persist_events (appends first)
    # 2. inject_attrs (appends second)
    # 3. thread_safe (appends third)
    # 4. authorize (appends fourth)

    defs = ComplexHandler._middlewares
    assert len(defs) == 4

    assert defs[0].middleware_class == EventStorePersistenceMiddleware
    assert "event_store" in defs[0].kwargs

    assert defs[1].middleware_class == AttributeInjectionMiddleware
    assert "injectors" in defs[1].kwargs

    assert defs[2].middleware_class == ThreadSafetyMiddleware
    assert defs[2].kwargs["entity_type"] == "T"

    assert defs[3].middleware_class == AuthorizationMiddleware
    assert defs[3].kwargs["permissions"] == ["read"]


# --- Missing Coverage Tests ---


@pytest.mark.asyncio
async def test_validator_legacy_fallback():
    # Test fallback to validator(data).validate() when validator(obj) fails

    class LegacyValidator:
        def __init__(self, data):
            self.data = data

        async def validate(self):
            # Return result object
            res = MagicMock()
            res.has_errors.return_value = False
            return res

    command = SimpleCommand()
    # Mock behavior: command.to_dict()
    command.to_dict = lambda: {"name": "legacy"}

    middleware = ValidatorMiddleware(validator_class=LegacyValidator)
    handler = AsyncMock(return_value="ok")

    wrapped = middleware.apply(handler, command)
    assert await wrapped() == "ok"


@pytest.mark.asyncio
async def test_authorization_middleware():
    from cqrs_ddd.middleware import AuthorizationMiddleware

    middleware = AuthorizationMiddleware(
        permissions=["read"], resource_type="file", resource_id_attr="id"
    )
    # Just verify it passes through for now (placeholder)
    handler = AsyncMock(return_value=CommandResponse(result="allowed"))
    wrapped = middleware.apply(handler, SimpleCommand())
    assert await wrapped() == CommandResponse(result="allowed")


@pytest.mark.asyncio
async def test_thread_safety_legacy_callable_strategy():
    # Strategy is a callable returning the lock handler
    mock_lock = MagicMock()
    mock_lock.acquire = AsyncMock(return_value="t")
    mock_lock.release = AsyncMock()

    def strategy_factory():
        return mock_lock

    middleware = ThreadSafetyMiddleware(
        entity_type="T", entity_id_attr="name", lock_strategy=strategy_factory
    )
    wrapped = middleware.apply(
        AsyncMock(return_value=CommandResponse(result="ok")), SimpleCommand(name="a")
    )
    await wrapped()

    mock_lock.acquire.assert_awaited()


@pytest.mark.asyncio
async def test_thread_safety_no_strategy():
    # Should skip with warning
    middleware = ThreadSafetyMiddleware("T", "id", lock_strategy=None)
    # Patch logging.getLogger to capture the warning
    with patch("logging.getLogger") as mock_get_log:
        mock_logger = MagicMock()
        mock_get_log.return_value = mock_logger

        wrapped = middleware.apply(
            AsyncMock(return_value=CommandResponse(result="ok")), SimpleCommand()
        )
        assert await wrapped() == CommandResponse(result="ok")
        mock_logger.warning.assert_called()


@pytest.mark.asyncio
async def test_event_store_persistence_error_handling():
    # 1. No event store -> skip
    middleware = EventStorePersistenceMiddleware(event_store=None)
    res_obj = CommandResponse(result="ok", events=["e"])
    handler = AsyncMock(return_value=res_obj)

    with patch("logging.getLogger") as mock_get_log:
        mock_logger = MagicMock()
        mock_get_log.return_value = mock_logger

        wrapped = middleware.apply(handler, SimpleCommand())
        await wrapped()
        mock_logger.warning.assert_called_with(
            "EventStorePersistenceMiddleware: No event store provided, skipping persistence"
        )

    # 2. Append raises exception -> caught
    mock_store = MagicMock()
    mock_store.append_batch = AsyncMock(side_effect=Exception("DB fail"))
    middleware2 = EventStorePersistenceMiddleware(event_store=mock_store)

    with patch("logging.getLogger") as mock_get_log2:
        mock_logger2 = MagicMock()
        mock_get_log2.return_value = mock_logger2

        # Patch enrich to avoid needing real events
        with patch(
            "cqrs_ddd.domain_event.enrich_event_metadata", side_effect=lambda e, **k: e
        ):
            wrapped2 = middleware2.apply(handler, SimpleCommand())
            await wrapped2()
            mock_logger2.error.assert_called()


@pytest.mark.asyncio
async def test_event_store_propagation_immutable_error():
    # If result object is immutable/tuple, setattr might fail -> should pass
    middleware = EventStorePersistenceMiddleware(event_store=MagicMock())

    # Result is a tuple (immutable)
    handler = AsyncMock(return_value=("foo",))
    wrapped = middleware.apply(handler, SimpleCommand())

    assert await wrapped() == ("foo",)
