import pytest
from typing import List, Union, Any
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock

from cqrs_ddd.persistence_dispatcher import (
    PersistenceDispatcher,
    OperationPersistence,
    RetrievalPersistence,
    QueryPersistence,
    _modification_handlers,
    _retrieval_handlers,
    _query_handlers,
)

# --- Dummies ---


@dataclass
class MyEntity:
    id: str
    name: str


@dataclass
class CreateEntityMod:
    entity: MyEntity


@dataclass
class UpdateEntityMod:
    entity: MyEntity


@dataclass
class EntityDTO:
    id: str
    name: str


# --- Handlers ---


class CreateEntityPersistence(OperationPersistence[CreateEntityMod]):
    priority = 100

    async def persist(self, modification: CreateEntityMod, uow: Any) -> str:
        return modification.entity.id


class UnionPersistence(OperationPersistence[Union[CreateEntityMod, UpdateEntityMod]]):
    priority = 10

    async def persist(self, mod, uow):
        return "high_priority"


class MyEntityRetrieval(RetrievalPersistence[MyEntity]):
    entity_name = "MyEntity"

    async def retrieve(self, ids: List[str], uow: Any) -> List[MyEntity]:
        # Simple mock implementation
        return [MyEntity(id=i, name=f"Entity {i}") for i in ids]


class DTOQuery(QueryPersistence[EntityDTO]):
    entity_name = "MyEntity"  # For cache

    async def fetch(self, ids: List[str], uow: Any) -> List[EntityDTO]:
        return [EntityDTO(id=i, name=f"DTO {i}") for i in ids]


# --- Tests ---


@pytest.fixture(autouse=True)
def clean_registries():
    # Clear registries before each test (simulating fresh start)
    # Since modules are imported once, we need to manually clear dicts
    # Note: But classes are defined at module level, so they register on import.
    # To test registration logic properly, we might need to define classes inside tests,
    # OR rely on module-level definitions and just check they are there.
    # Let's clean and RE-REGISTER dummy handlers manually if needed, or define local classes.

    # Actually, defining classes inside mocks doesn't trigger __init_subclass__ unless they inherit
    # from the imported base.

    # Strategy: We'll trust the module-level mocks above are registered.
    # We won't clear registries completely to avoid breaking other tests running in same process if potential parallel implementation.
    # But for unit tests here, we want isolation.

    # Save original state
    orig_mod = _modification_handlers.copy()
    orig_ret = _retrieval_handlers.copy()
    orig_qry = _query_handlers.copy()

    yield

    # Restore
    _modification_handlers.clear()
    _modification_handlers.update(orig_mod)
    _retrieval_handlers.clear()
    _retrieval_handlers.update(orig_ret)
    _query_handlers.clear()
    _query_handlers.update(orig_qry)


@pytest.mark.asyncio
async def test_registration_and_priority():
    # UnionPersistence has priority 10
    # CreateEntityPersistence has priority 0
    handlers = _modification_handlers[CreateEntityMod]
    assert len(handlers) >= 2

    # High priority first
    assert handlers[0].handler_cls == CreateEntityPersistence

    # Check retrieval
    assert MyEntity in _retrieval_handlers


@pytest.mark.asyncio
async def test_apply_write_flow():
    uow_factory = MagicMock()
    mock_uow = AsyncMock()
    uow_factory.return_value.__aenter__.return_value = mock_uow

    cache_service = MagicMock()
    cache_service.delete_batch = AsyncMock()

    dispatcher = PersistenceDispatcher(uow_factory, cache_service=cache_service)

    mod = CreateEntityMod(entity=MyEntity(id="e1", name="Test"))

    # Should call UnionPersistence (priority 10)
    # Logic in apply:
    # 1. Resolve handlers (CreateEntityPersistence @ 100, UnionPersistence @ 10)
    # 2. Execute handlers in priority order (CreateEntityPersistence first)
    # 3. Return result from the HIGHEST priority handler (e1)

    # CreateEntityPersistence runs first (priority 100) and returns "e1".

    result = await dispatcher.apply(mod)

    assert result == "e1"

    # Verify cache invalidation
    # Keys: MyEntity:e1, MyEntity:query:e1
    cache_service.delete_batch.assert_called_once()
    call_args = cache_service.delete_batch.call_args[0][0]
    assert "MyEntity:e1" in call_args
    assert "MyEntity:query:e1" in call_args


@pytest.mark.asyncio
async def test_fetch_domain_read_through_cache():
    uow_factory = MagicMock()
    mock_uow = AsyncMock()
    uow_factory.return_value.__aenter__.return_value = mock_uow

    cache_service = MagicMock()
    cache_service.get_batch = AsyncMock()
    cache_service.set_batch = AsyncMock()

    dispatcher = PersistenceDispatcher(uow_factory, cache_service=cache_service)

    ids = ["1", "2", "3"]

    # Scenario: 1 is cached, 2 and 3 are missing
    entity1 = MyEntity(id="1", name="Cached")
    cache_service.get_batch.return_value = [entity1, None, None]

    results = await dispatcher.fetch_domain(MyEntity, ids)

    assert len(results) == 3
    assert results[0] == entity1
    assert results[1].id == "2"  # From DB
    assert results[2].id == "3"  # From DB

    # Verify DB fetch called only for missing
    # We can't spy easily on MyEntityRetrieval.retrieve unless we mock handler factory
    # But we can verify set_batch called for 2 items

    cache_service.set_batch.assert_called_once()
    set_args = cache_service.set_batch.call_args[0][0]  # List of dicts
    assert len(set_args) == 2
    assert set_args[0]["cache_key"] == "MyEntity:2"


@pytest.mark.asyncio
async def test_fetch_query_read_through():
    # Similar to domain but checking query path
    uow_call = MagicMock()
    dispatcher = PersistenceDispatcher(uow_call, cache_service=MagicMock())
    dispatcher._cache_service.get_batch = AsyncMock(return_value=[None])
    dispatcher._cache_service.set_batch = AsyncMock()

    res = await dispatcher.fetch(EntityDTO, ["100"])

    assert len(res) == 1
    assert res[0].name == "DTO 100"

    # Cache key suffix
    dispatcher._cache_service.set_batch.assert_called()
    key = dispatcher._cache_service.set_batch.call_args[0][0][0]["cache_key"]
    assert ":query:100" in key


@pytest.mark.asyncio
async def test_auto_discovery_generic_inference():
    # Test internal extraction logic implicitly via registration
    # Verify that generic types are extracted correctly

    # UnionPersistence[Union[CreateEntityMod, UpdateEntityMod]]
    # Should be registered for BOTH

    entries_create = _modification_handlers[CreateEntityMod]
    assert any(e.handler_cls == UnionPersistence for e in entries_create)

    entries_update = _modification_handlers[UpdateEntityMod]
    assert any(e.handler_cls == UnionPersistence for e in entries_update)
