
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List, Any, Union, Generic, TypeVar
from cqrs_ddd.persistence_dispatcher import (
    OperationPersistence,
    RetrievalPersistence,
    QueryPersistence,
    PersistenceDispatcher,
    HandlerEntry,
    list_modification_handlers,
    list_retrieval_handlers,
    list_query_handlers,
    _modification_handlers,
    _retrieval_handlers,
    _query_handlers
)

# Dummy types for testing
@pytest.fixture(autouse=True)
def clean_registries():
    _modification_handlers.clear()
    _retrieval_handlers.clear()
    _query_handlers.clear()
    yield
    _modification_handlers.clear()
    _retrieval_handlers.clear()
    _query_handlers.clear()

class DummyMod: pass
class DummyEntity:
    def __init__(self, id): self.id = id
class DummyResult:
    def __init__(self, id): self.id = id

def test_handler_registration_duplicate_coverage(caplog):
    """Cover duplicate registration branch in _register_handler."""
    class Handler(OperationPersistence[DummyMod]):
        async def persist(self, mod, uow): return 1
        
    from cqrs_ddd.persistence_dispatcher import _register_handler
    # Register again manually
    _register_handler(_modification_handlers, DummyMod, Handler, 0)
    # Should return early and not add a duplicate (already added by __init_subclass__)
    assert len(_modification_handlers[DummyMod]) == 1

def test_extract_generic_args_typevar_coverage():
    """Cover TypeVar branch in _extract_generic_args."""
    T = TypeVar('T')
    class GenericHandler(OperationPersistence[T]):
        async def persist(self, mod, uow): return 1
    
    # GenericHandler shouldn't register anything because T is a TypeVar
    assert DummyMod not in _modification_handlers

def test_introspection_coverage():
    """Cover list_*_handlers functions."""
    class M(OperationPersistence[DummyMod]):
        async def persist(self, m, uow): pass
    class R(RetrievalPersistence[DummyEntity]):
        async def retrieve(self, ids, uow): pass
    class Q(QueryPersistence[DummyResult]):
        async def fetch(self, ids, uow): pass
        
    assert "DummyMod" in list_modification_handlers()
    assert "DummyEntity" in list_retrieval_handlers()
    assert "DummyResult" in list_query_handlers()

def test_extract_generic_args_union_coverage():
    """Cover Union branch in _extract_generic_args."""
    class UnionHandler(OperationPersistence[Union[DummyMod, DummyEntity]]):
        async def persist(self, mod, uow): return 1
    
    assert DummyMod in _modification_handlers
    assert DummyEntity in _modification_handlers

@pytest.mark.asyncio
async def test_persistence_dispatcher_query_read_through_coverage():
    """Cover query side read-through cache and unit_of_work branch."""
    cache = MagicMock()
    cache.get_batch = AsyncMock(return_value=[None])
    cache.set_batch = AsyncMock()
    
    class Q(QueryPersistence[DummyResult]):
        entity_name = "DummyQ"
        async def fetch(self, ids, uow): return [DummyResult(ids[0])]
        
    uow = MagicMock()
    # Mocking async context manager
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)
    
    dispatcher = PersistenceDispatcher(uow_factory=lambda: uow, cache_service=cache)
    
    # 1. With unit_of_work (hits line 362)
    res = await dispatcher.fetch(DummyResult, [1], unit_of_work=uow)
    assert res[0].id == 1
    
    # 2. Without unit_of_work (hits line 364)
    cache.get_batch = AsyncMock(return_value=[None])
    res = await dispatcher.fetch(DummyResult, [2])
    assert res[0].id == 2

@pytest.mark.asyncio
async def test_persistence_dispatcher_fetch_domain_one_empty():
    """Cover fetch_domain_one with empty result."""
    class R(RetrievalPersistence[DummyEntity]):
        async def retrieve(self, ids, uow): return []
        
    dispatcher = PersistenceDispatcher(uow_factory=MagicMock())
    with patch("cqrs_ddd.persistence_dispatcher._get_handlers", return_value=[R]):
        res = await dispatcher.fetch_domain_one(DummyEntity, 1)
        assert res is None

@pytest.mark.asyncio
async def test_base_classes_not_implemented():
    """Cover NotImplementedError in base classes via super() calls."""
    class M(OperationPersistence[DummyMod]):
        async def persist(self, m, uow):
            return await super().persist(m, uow)
    
    class R(RetrievalPersistence[DummyEntity]):
        async def retrieve(self, ids, uow):
            return await super().retrieve(ids, uow)
        
    class Q(QueryPersistence[DummyResult]):
        async def fetch(self, ids, uow):
            return await super().fetch(ids, uow)
        
    m = M()
    r = R()
    q = Q()
    
    with pytest.raises(NotImplementedError):
        await m.persist(None, None)
    with pytest.raises(NotImplementedError):
        await r.retrieve([], None)
    with pytest.raises(NotImplementedError):
        await q.fetch([], None)

@pytest.mark.asyncio
async def test_persistence_dispatcher_transaction():
    """Cover dispatcher.transaction()."""
    uow = MagicMock()
    uow_factory = MagicMock(return_value=uow)
    dispatcher = PersistenceDispatcher(uow_factory=uow_factory)
    assert dispatcher.transaction() == uow

@pytest.mark.asyncio
async def test_persistence_dispatcher_no_handler_errors():
    """Cover ValueError when no handler is registered."""
    dispatcher = PersistenceDispatcher(uow_factory=MagicMock())
    
    with pytest.raises(ValueError, match="No handler registered for DummyMod"):
        await dispatcher.apply(DummyMod())
        
    with pytest.raises(ValueError, match="No retrieval handler registered for DummyEntity"):
        await dispatcher.fetch_domain(DummyEntity, [1])
        
    with pytest.raises(ValueError, match="No query handler registered for DummyResult"):
        await dispatcher.fetch(DummyResult, [1])

@pytest.mark.asyncio
async def test_persistence_dispatcher_with_provided_uow():
    """Cover apply/fetch with provided unit_of_work."""
    class M(OperationPersistence[DummyMod]):
        async def persist(self, mod, uow): return 1
        
    class R(RetrievalPersistence[DummyEntity]):
        async def retrieve(self, ids, uow): return [DummyEntity(1)]
        
    class Q(QueryPersistence[DummyResult]):
        async def fetch(self, ids, uow): return [DummyResult(1)]
        
    dispatcher = PersistenceDispatcher(uow_factory=MagicMock())
    uow = MagicMock()
    
    # 1. apply
    res = await dispatcher.apply(DummyMod(), unit_of_work=uow)
    assert res == 1
    
    # 2. fetch_domain
    res = await dispatcher.fetch_domain(DummyEntity, [1], unit_of_work=uow)
    assert res[0].id == 1
    
    # 3. fetch (query)
    res = await dispatcher.fetch(DummyResult, [1], unit_of_work=uow)
    assert res[0].id == 1

@pytest.mark.asyncio
async def test_persistence_dispatcher_fetch_one_helpers():
    """Cover fetch_domain_one and fetch_one."""
    class R(RetrievalPersistence[DummyEntity]):
        async def retrieve(self, ids, uow): return [DummyEntity(ids[0])]
        
    class Q(QueryPersistence[DummyResult]):
        async def fetch(self, ids, uow): return [DummyResult(ids[0])]

    uow = MagicMock()
    uow.__aenter__.return_value = uow
    uow_factory = MagicMock(return_value=uow)
    dispatcher = PersistenceDispatcher(uow_factory=uow_factory)
    
    # fetch_domain_one
    res = await dispatcher.fetch_domain_one(DummyEntity, 123)
    assert res.id == 123
    
    # fetch_one (query)
    res = await dispatcher.fetch_one(DummyResult, 456)
    assert res.id == 456
    
    # Empty results
    with patch.object(dispatcher, "fetch_domain", AsyncMock(return_value=[])):
        assert await dispatcher.fetch_domain_one(DummyEntity, 1) is None
    with patch.object(dispatcher, "fetch", AsyncMock(return_value=[])):
        assert await dispatcher.fetch_one(DummyResult, 1) is None

@pytest.mark.asyncio
async def test_execute_read_through_cache_errors_and_shortcuts():
    """Cover error paths and shortcuts in _execute_read_through."""
    cache = MagicMock()
    cache.get_batch = AsyncMock(side_effect=Exception("Cache down"))
    cache.set_batch = AsyncMock(side_effect=Exception("Cache down set"))
    
    class R(RetrievalPersistence[DummyEntity]):
        entity_name = "Dummy"
        async def retrieve(self, ids, uow): return [DummyEntity(i) for i in ids]
        
    uow = MagicMock()
    uow.__aenter__.return_value = uow
    dispatcher = PersistenceDispatcher(uow_factory=lambda: uow, cache_service=cache)
    
    # 1. Cache get error
    res = await dispatcher.fetch_domain(DummyEntity, [1])
    assert len(res) == 1
    assert res[0].id == 1
    
    # 2. No missing ids (shortcut)
    cache.get_batch = AsyncMock(return_value=[DummyEntity(1)])
    res = await dispatcher.fetch_domain(DummyEntity, [1])
    assert res[0].id == 1
    
    # 3. Cache set failure coverage
    cache.get_batch = AsyncMock(return_value=[None])
    res = await dispatcher.fetch_domain(DummyEntity, [1])
    assert res[0].id == 1

@pytest.mark.asyncio
async def test_invalidate_cache_error_coverage(caplog):
    """Cover exception in _invalidate_cache."""
    cache = MagicMock()
    cache.delete_batch = AsyncMock(side_effect=Exception("Delete failed"))
    
    class M(OperationPersistence[DummyMod]):
        async def persist(self, mod, uow): return 1
        
    mod = DummyMod()
    mod.entity = MagicMock()
    
    dispatcher = PersistenceDispatcher(uow_factory=MagicMock(), cache_service=cache)
    # We call it indirectly via apply
    with patch.object(dispatcher, "_uow_factory"):
        # Manual call to avoid setting up UOW factory mock for async with
        await dispatcher._invalidate_cache(mod, 1)
        
    import logging
    assert "Cache invalidation failed" in caplog.text

@pytest.mark.asyncio
async def test_apply_cache_invalidation_logic():
    """Cover apply invalidate cache branch."""
    class M(OperationPersistence[DummyMod]):
        async def persist(self, mod, uow): return 1
        
    cache = MagicMock()
    cache.delete_batch = AsyncMock()
    
    uow = MagicMock()
    uow.__aenter__.return_value = uow
    dispatcher = PersistenceDispatcher(uow_factory=lambda: uow, cache_service=cache)
    
    mod = DummyMod()
    mod.entity = MagicMock()
    
    # Should invalidate cache
    await dispatcher.apply(mod)
    assert cache.delete_batch.called

@pytest.mark.asyncio
async def test_apply_provided_uow_execute():
    """Ensure execute(uow) is called when unit_of_work is provided."""
    class M(OperationPersistence[DummyMod]):
        async def persist(self, mod, uow): return 1
        
    dispatcher = PersistenceDispatcher(uow_factory=MagicMock())
    uow = MagicMock()
    
    res = await dispatcher.apply(DummyMod(), unit_of_work=uow)
    assert res == 1
