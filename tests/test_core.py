from dataclasses import dataclass
from unittest.mock import patch
from typing import Union

from cqrs_ddd.core import (
    Command,
    Query,
    CommandResponse,
    QueryResponse,
    CommandHandler,
    QueryHandler,
    EventHandler,
    AbstractDomainEvent,
)


# --- Test Commands and Queries ---


def test_command_initialization():
    @dataclass(kw_only=True)
    class CreateUser(Command):
        username: str

    cmd = CreateUser(username="testuser")

    assert cmd.username == "testuser"
    assert cmd.command_id is not None
    assert isinstance(cmd.command_id, str)
    assert len(cmd.command_id) > 0
    assert cmd.correlation_id is None


def test_command_correlation_id():
    @dataclass(kw_only=True)
    class CreateUser(Command):
        username: str

    cmd = CreateUser(username="testuser")
    cmd.correlation_id = "123-abc"
    assert cmd.correlation_id == "123-abc"


def test_command_to_dict():
    @dataclass(kw_only=True)
    class CreateUser(Command):
        username: str
        age: int

    cmd = CreateUser(username="testuser", age=30)
    data = cmd.to_dict()

    # Should exclude private fields starting with _
    assert data == {"username": "testuser", "age": 30}
    assert "_command_id" not in data
    assert "_correlation_id" not in data


def test_query_initialization():
    @dataclass(kw_only=True)
    class GetUser(Query):
        user_id: str

    query = GetUser(user_id="u123")

    assert query.user_id == "u123"
    assert query.query_id is not None
    assert isinstance(query.query_id, str)
    assert query.correlation_id is None


def test_query_correlation_id():
    @dataclass(kw_only=True)
    class GetUser(Query):
        user_id: str

    query = GetUser(user_id="u123")
    query.correlation_id = "xyz-789"
    assert query.correlation_id == "xyz-789"


def test_query_to_dict():
    @dataclass(kw_only=True)
    class GetUser(Query):
        user_id: str
        active: bool

    query = GetUser(user_id="u123", active=True)
    data = query.to_dict()

    assert data == {"user_id": "u123", "active": True}
    assert "_query_id" not in data


# --- Test Responses ---


def test_command_response():
    resp = CommandResponse(result="success", events=["event1", "event2"])

    assert resp.result == "success"
    assert resp.response == "success"  # Test alias
    assert resp.events == ["event1", "event2"]
    assert resp.domain_events == ["event1", "event2"]  # Test alias


def test_query_response():
    resp = QueryResponse(result={"data": 123})

    assert resp.result == {"data": 123}
    assert resp.response == {"data": 123}  # Test alias


# --- Test Handler Registration ---


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_command_handler")
def test_command_handler_registration(mock_register, mock_logger):
    @dataclass(kw_only=True)
    class MyCommand(Command):
        val: int

    class MyCommandHandler(CommandHandler[str]):
        async def handle(self, command: MyCommand) -> CommandResponse[str]:
            return CommandResponse(result="ok")

    # Verification
    mock_register.assert_called_once_with(MyCommand, MyCommandHandler)
    assert MyCommandHandler._handles_command == MyCommand


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_command_handler")
def test_command_handler_registration_failure_wrong_type(mock_register, mock_logger):
    # Should log warning because First argument is not a Command
    class InvalidCommand:
        pass

    class BadHandler(CommandHandler[str]):
        async def handle(self, command: InvalidCommand) -> CommandResponse[str]:
            return CommandResponse(result="error")

    mock_register.assert_not_called()
    mock_logger.warning.assert_called()
    assert (
        "does not inherit from AbstractCommand" in mock_logger.warning.call_args[0][0]
    )


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_command_handler")
def test_command_handler_registration_no_annotation(mock_register, mock_logger):
    class NoTypeHandler(CommandHandler[str]):
        async def handle(self, command) -> CommandResponse[str]:
            return CommandResponse(result="error")

    mock_register.assert_not_called()
    mock_logger.warning.assert_called()
    assert "must have a type annotation" in mock_logger.warning.call_args[0][0]


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_query_handler")
def test_query_handler_registration(mock_register, mock_logger):
    @dataclass(kw_only=True)
    class MyQuery(Query):
        val: int

    class MyQueryHandler(QueryHandler[str]):
        async def handle(self, query: MyQuery) -> QueryResponse[str]:
            return QueryResponse(result="ok")

    # Verification
    mock_register.assert_called_once_with(MyQuery, MyQueryHandler)
    assert MyQueryHandler._handles_query == MyQuery


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_event_handler")
def test_event_handler_registration_single(mock_register, mock_logger):
    @dataclass(kw_only=True)
    class MyEvent(AbstractDomainEvent):
        name: str

    class MyEventHandler(EventHandler[MyEvent]):
        async def handle(self, event: MyEvent) -> None:
            pass

    # Verification
    mock_register.assert_called_once_with(MyEvent, MyEventHandler, False)
    assert MyEventHandler._handles_event == MyEvent


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_event_handler")
def test_event_handler_registration_union(mock_register, mock_logger):
    @dataclass(kw_only=True)
    class EventA(AbstractDomainEvent):
        val: int

    @dataclass(kw_only=True)
    class EventB(AbstractDomainEvent):
        val: int

    class UnionEventHandler(EventHandler[Union[EventA, EventB]]):
        async def handle(self, event: Union[EventA, EventB]) -> None:
            pass

    # Verification - should be called for both events
    assert mock_register.call_count == 2
    mock_register.assert_any_call(EventA, UnionEventHandler, False)
    mock_register.assert_any_call(EventB, UnionEventHandler, False)

    # Internal tracking stores the first one or tuple - implementation says first one
    assert UnionEventHandler._handles_event == EventA


@patch("cqrs_ddd.core.logger")
@patch("cqrs_ddd.handler_registry.register_event_handler")
def test_event_handler_priority(mock_register, mock_logger):
    @dataclass(kw_only=True)
    class PriorityEvent(AbstractDomainEvent):
        pass

    class PriorityEventHandler(EventHandler[PriorityEvent]):
        is_priority = True

        async def handle(self, event: PriorityEvent) -> None:
            pass

    mock_register.assert_called_once_with(PriorityEvent, PriorityEventHandler, True)
