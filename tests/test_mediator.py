import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass

from cqrs_ddd.mediator import Mediator, _current_uow
from cqrs_ddd.core import Command, Query, CommandResponse, CommandHandler, QueryHandler
from cqrs_ddd.protocols import UnitOfWork, EventDispatcher

# --- Helper Objects ---


@dataclass
class MyCommand(Command):
    data: str

    # Initialize correlation_id manually if needed for tests
    # or rely on dataclass behavior but kw_only interactions can be complex


@dataclass
class MyQuery(Query):
    q_data: str


@dataclass
class MyEvent:
    name: str


class MyCommandHandler(CommandHandler[str]):
    async def handle(self, command: MyCommand) -> CommandResponse[str]:
        return CommandResponse(
            result=f"handled {command.data}", events=[MyEvent("event1")]
        )


class MyQueryHandler(QueryHandler[str]):
    async def handle(self, query: MyQuery) -> str:
        return f"result {query.q_data}"


class SimpleUoW(UnitOfWork):
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.session = MagicMock()
        self.session.flush = AsyncMock()

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()


# --- Fixtures ---


@pytest.fixture
def mock_uow_factory():
    uow = SimpleUoW()
    factory = MagicMock(return_value=uow)
    return factory, uow


@pytest.fixture
def mock_event_dispatcher():
    dispatcher = MagicMock(spec=EventDispatcher)
    dispatcher.dispatch_priority = AsyncMock()
    dispatcher.dispatch_background = AsyncMock()
    return dispatcher


@pytest.fixture
def mediator(mock_uow_factory, mock_event_dispatcher):
    factory, _ = mock_uow_factory
    return Mediator(uow_factory=factory, event_dispatcher=mock_event_dispatcher)


@pytest.fixture(autouse=True)
def reset_context():
    token = _current_uow.set(None)
    yield
    _current_uow.reset(token)


# --- Tests ---


@pytest.mark.asyncio
async def test_manual_registration_and_dispatch(mediator):
    handler = MyCommandHandler()
    mediator.register(MyCommand, handler)

    cmd = MyCommand(data="test")
    response = await mediator.send(cmd)

    assert response.result == "handled test"
    assert len(response.events) == 1


@pytest.mark.asyncio
async def test_lazy_lookup_from_registry(mediator):
    # Patch the module where get_registered_handlers is defined
    with patch(
        "cqrs_ddd.handler_registry.get_registered_handlers"
    ) as mock_get_registry:
        mock_get_registry.return_value = {
            "commands": {MyCommand: MyCommandHandler},
            "queries": {},  # Explicitly set empty dicts to avoid key errors if accessed
            "events": {},
        }

        cmd = MyCommand(data="lazy")
        response = await mediator.send(cmd)

        assert response.result == "handled lazy"


@pytest.mark.asyncio
async def test_uow_creation_and_commit(mock_uow_factory, mock_event_dispatcher):
    factory, uow = mock_uow_factory
    mediator = Mediator(uow_factory=factory, event_dispatcher=mock_event_dispatcher)

    handler = MyCommandHandler()
    mediator.register(MyCommand, handler)

    cmd = MyCommand(data="uow_test")
    await mediator.send(cmd)

    # Verify factory called
    factory.assert_called_once()
    # Verify commit (via __aexit__)
    assert uow.committed is True
    assert uow.rolled_back is False
    # Verify events dispatched
    mock_event_dispatcher.dispatch_priority.assert_awaited_once()  # 1 event
    mock_event_dispatcher.dispatch_background.assert_awaited_once()


@pytest.mark.asyncio
async def test_nested_command_reuses_uow(mock_uow_factory):
    factory, uow = mock_uow_factory
    mediator = Mediator(uow_factory=factory)

    # Outer handler sends inner command
    class OuterCommand(Command):
        pass

    class InnerCommand(Command):
        pass

    inner_handler = AsyncMock()
    inner_handler.handle = AsyncMock(return_value=CommandResponse(result="inner"))

    outer_handler = AsyncMock()

    async def outer_handle(cmd):
        # reuse mediator to send inner
        return await mediator.send(InnerCommand())

    outer_handler.handle = outer_handle

    mediator.register(OuterCommand, outer_handler)
    mediator.register(InnerCommand, inner_handler)

    await mediator.send(OuterCommand())

    # Factory should be called only ONCE (for outer)
    factory.assert_called_once()
    assert uow.committed is True


@pytest.mark.asyncio
async def test_query_dispatch_no_uow(mock_uow_factory):
    factory, _ = mock_uow_factory
    mediator = Mediator(uow_factory=factory)

    handler = MyQueryHandler()
    mediator.register(MyQuery, handler)

    result = await mediator.send(MyQuery(q_data="query"))

    assert result == "result query"
    # UoW factory should NOT be called for queries
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_middleware_execution():
    mediator = Mediator()

    class MiddlewareHandler:
        _middlewares = [MagicMock()]  # Just a mock object in list

    # Fake middleware that modifies message
    mock_mw_instance = MagicMock()
    mock_mw_instance.apply = MagicMock(
        side_effect=lambda func, msg: func
    )  # Identity wrapper

    # We need to mock how mediator processes middlewares.
    # Mediator expects middleware definition or instance.

    handler = MyCommandHandler()
    # Inject middleware
    handler.__class__._middlewares = [mock_mw_instance]

    mediator.register(MyCommand, handler)

    await mediator.send(MyCommand(data="mw"))

    mock_mw_instance.apply.assert_called_once()


@pytest.mark.asyncio
async def test_container_integration():
    container = MagicMock()
    container.resolve_stuff.return_value = "resolved"

    # Mock UoW that is returned by factory
    mock_uow = MagicMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock()
    # Ensure session.flush is awaitable
    mock_uow.session.flush = AsyncMock()

    # Mock factory
    container.uow_factory.return_value = lambda: mock_uow

    # Mock core.event_dispatcher if accessed
    mock_dispatcher = MagicMock()
    mock_dispatcher.dispatch_background = AsyncMock()  # Make awaitable
    mock_dispatcher.dispatch_priority = AsyncMock()

    container.core.event_dispatcher.return_value = mock_dispatcher
    container.event_dispatcher = (
        None  # Ensure it falls back to core or handle both logic provided in mediator
    )

    # Define mock_resolver BEFORE using it or initializing Mediator
    mock_resolver = MagicMock(return_value=MyCommandHandler())

    # Ensure registry lookup returns empty so it proceeds to resolver
    with patch("cqrs_ddd.handler_registry.get_registered_handlers") as mock_registry:
        mock_registry.return_value = {"commands": {}, "queries": {}, "events": {}}

        mediator = Mediator(handler_resolver=mock_resolver, container=container)

        await mediator.send(MyCommand(data="di"))

    mock_resolver.assert_called_with(MyCommand)


@pytest.mark.asyncio
async def test_event_enrichment_correlation_id(mediator):
    handler = MyCommandHandler()
    mediator.register(MyCommand, handler)

    cmd = MyCommand(data="enrich")
    cmd.correlation_id = "corr-999"
    cmd._command_id = "cmd-888"  # set internal or property

    # Patch enrich_event_metadata in domain_event module (where it is defined)
    # Mediator imports it locally, so we target the source.
    with patch("cqrs_ddd.domain_event.enrich_event_metadata") as mock_enrich:
        # Pass through the event so logic continues
        mock_enrich.side_effect = lambda evt, **kwargs: evt

        response = await mediator.send(cmd)

        # Verify enrich called with correct IDs
        # The Mediator calls it for each event in priority AND background dispatch loops.
        assert mock_enrich.call_count >= 1
        args, kwargs = mock_enrich.call_args_list[0]
        # args[0] is the event
        assert kwargs["correlation_id"] == "corr-999"
        assert kwargs["causation_id"] == "cmd-888"

    # Response should inherit IDs logic is separate from events
    assert response.correlation_id == "corr-999"
    assert response.causation_id == "cmd-888"


@pytest.mark.asyncio
async def test_no_handler_error(mediator):
    class UnknownCmd(Command):
        pass

    with pytest.raises(ValueError, match="No handler registered"):
        await mediator.send(UnknownCmd())
