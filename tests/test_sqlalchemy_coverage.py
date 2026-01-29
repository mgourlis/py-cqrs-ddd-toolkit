
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from typing import Any, List, Optional, Type
from datetime import datetime, timezone
import sys
import importlib
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, select
from pydantic import BaseModel, ConfigDict

import cqrs_ddd.backends.sqlalchemy as sqlalchemy_mod
from cqrs_ddd.backends.sqlalchemy import (
    SQLAlchemyUnitOfWork, 
    create_uow_factory,
    SQLAlchemyOperationPersistence,
    SQLAlchemyRetrievalPersistence,
    SQLAlchemyQueryPersistence,
    SQLAlchemyEntityMixin,
    ConcurrencyError
)

# --- Test Models ---

class Base(DeclarativeBase):
    pass

class DummyModel(Base, SQLAlchemyEntityMixin):
    __tablename__ = "dummy"
    name: Mapped[str] = mapped_column(String)

@dataclass
class DummyEntity:
    id: str
    name: str
    version: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None

class DummyPydantic(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    name: str
    version: int = 0

# --- Fixtures ---

@pytest.fixture
async def engine():
    import sqlite3
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

@pytest.fixture
def mock_session():
    session = AsyncMock()
    session.execute = AsyncMock()
    return session

# --- Unit of Work Tests ---

@pytest.mark.asyncio
async def test_sqlalchemy_uow_lifecycle(session_factory):
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    
    async with uow:
        assert isinstance(uow.session, AsyncSession)
        # Adding a record to verify transaction
        uow.session.add(DummyModel(id="1", name="test"))
    
    # Verify commit
    async with session_factory() as session:
        result = await session.execute(select(DummyModel).where(DummyModel.id == "1"))
        assert result.scalar_one_or_none() is not None

@pytest.mark.asyncio
async def test_sqlalchemy_uow_rollback(session_factory):
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    
    try:
        async with uow:
            uow.session.add(DummyModel(id="2", name="rollback"))
            raise RuntimeError("Boom")
    except RuntimeError:
        pass
    
    # Verify rollback
    async with session_factory() as session:
        result = await session.execute(select(DummyModel).where(DummyModel.id == "2"))
        assert result.scalar_one_or_none() is None

@pytest.mark.asyncio
async def test_sqlalchemy_uow_errors(session_factory):
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    
    # Error: Access session before start
    with pytest.raises(RuntimeError, match="not started"):
        _ = uow.session
        
    # Error: No factory
    uow_no_factory = SQLAlchemyUnitOfWork()
    with pytest.raises(RuntimeError, match="No session factory"):
        async with uow_no_factory:
            pass

@pytest.mark.asyncio
async def test_sqlalchemy_uow_factory(engine, session_factory):
    # Factory from engine
    factory = create_uow_factory(engine=engine)
    assert isinstance(factory(), SQLAlchemyUnitOfWork)
    
    # Factory from session_factory
    factory2 = create_uow_factory(session_factory=session_factory)
    assert isinstance(factory2(), SQLAlchemyUnitOfWork)
    
    # Error: Missing args
    with pytest.raises(ValueError, match="Either engine or session_factory"):
        create_uow_factory()

# --- Persistence Tests ---

class MyOpPersistence(SQLAlchemyOperationPersistence[DummyEntity]):
    model_class = DummyModel
    async def persist(self, entity, uow): return entity.id

class MyRetrievalPersistence(SQLAlchemyRetrievalPersistence[DummyEntity]):
    model_class = DummyModel
    entity_class = DummyEntity

class MyQueryPersistence(SQLAlchemyQueryPersistence[DummyPydantic]):
    model_class = DummyModel
    dto_class = DummyPydantic

@pytest.mark.asyncio
async def test_persistence_conversions(session_factory):
    op = MyOpPersistence()
    retrieval = MyRetrievalPersistence()
    query = MyQueryPersistence()
    
    # 1. to_model (from dataclass)
    entity = DummyEntity(id="101", name="dc", version=1)
    model = op.to_model(entity)
    assert model.id == "101"
    assert model.name == "dc"
    
    # 2. to_model (from Pydantic)
    pe = DummyPydantic(id="102", name="pydantic")
    model2 = op.to_model(pe)
    assert model2.id == "102"
    
    # 3. to_entity (to dataclass)
    db_model = DummyModel(id="103", name="db", version=5)
    entity_dc = retrieval.to_entity(db_model)
    assert entity_dc.id == "103"
    assert entity_dc.name == "db"
    
    # 4. to_dto (to pydantic)
    dto = query.to_dto(db_model)
    assert dto.id == "103"
    assert dto.name == "db"

@pytest.mark.asyncio
async def test_persistence_operations(session_factory):
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    async with uow:
        uow.session.add_all([
            DummyModel(id="1", name="A"),
            DummyModel(id="2", name="B")
        ])
    
    retrieval = MyRetrievalPersistence()
    query = MyQueryPersistence()
    
    async with uow:
        # Retrieve domain
        entities = await retrieval.retrieve(["1", "2"], uow)
        assert len(entities) == 2
        assert entities[0].name in ["A", "B"]
        
        # Fetch query
        dtos = await query.fetch(["1"], uow)
        assert len(dtos) == 1
        assert dtos[0].name == "A"

@pytest.mark.asyncio
async def test_concurrency_checking(session_factory):
    op = MyOpPersistence()
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    
    async with uow:
        uow.session.add(DummyModel(id="sync", name="old", version=1))
    
    # Success: version matches (expecting DB 1, so entity version 2)
    entity = DummyEntity(id="sync", name="new", version=2)
    async with uow:
        await op.check_version(uow.session, entity)
    
    # Fail: version mismatch (expecting DB 1, but entity version is also 1? no, entity 1 means expect DB 0)
    # If entity.version = 5, check_version expects DB.version = 4
    entity_fail = DummyEntity(id="sync", name="fail", version=9)
    async with uow:
        with pytest.raises(ConcurrencyError):
            await op.check_version(uow.session, entity_fail)

# --- Fallback Tests ---

@pytest.mark.asyncio
async def test_sqlalchemy_fallback_loading():
    with patch.dict(sys.modules, {
        "sqlalchemy": None, 
        "sqlalchemy.ext.asyncio": None,
        "sqlalchemy.orm": None,
        "sqlalchemy.sql": None
    }):
        importlib.reload(sqlalchemy_mod)
        assert sqlalchemy_mod.HAS_SQLALCHEMY is False
        with pytest.raises(ImportError, match="SQLAlchemy is required"):
            SQLAlchemyUnitOfWork()
        with pytest.raises(ImportError, match="SQLAlchemy is required"):
            create_uow_factory()
    
    # Restore
    importlib.reload(sqlalchemy_mod)
    assert sqlalchemy_mod.HAS_SQLALCHEMY is True

@pytest.mark.asyncio
async def test_sqlalchemy_uow_flush_edge(session_factory):
    uow = SQLAlchemyUnitOfWork(session_factory=session_factory)
    await uow.flush() # Should not raise even if not started (line 110)
    async with uow:
        await uow.flush()

@pytest.mark.asyncio
async def test_persistence_edge_cases(mock_session):
    # Use MyOpPersistence instead of SQLAlchemyOperationPersistence to avoid ABC error
    op = MyOpPersistence()
    # to_model returns raw if not a dict/pydantic/dataclass (line 230)
    assert op.to_model("raw_string") == "raw_string"
    
    # check_version returns if no id/version (line 252)
    await op.check_version(mock_session, object()) 
    
    # check_version returns if no model_class (line 249)
    class NoModelOp(MyOpPersistence): model_class = None
    await NoModelOp().check_version(mock_session, DummyEntity(id="1", name="a"))

@pytest.mark.asyncio
async def test_retrieval_query_fallbacks():
    # Retrieval failures (line 278)
    class FailRetrieval(SQLAlchemyRetrievalPersistence): pass
    with pytest.raises(NotImplementedError):
        FailRetrieval().to_entity(MagicMock())
        
    # Query failures (line 319)
    class FailQuery(SQLAlchemyQueryPersistence): pass
    with pytest.raises(NotImplementedError):
        FailQuery().to_dto(MagicMock())
        
    # Pydantic V1/dict fallback (lines 225, 290, 327, 330)
    class MockV1:
        @classmethod
        def from_orm(cls, obj): return "v1"
        def dict(self): return {"id": "v1"}
    
    class V1Query(SQLAlchemyQueryPersistence): dto_class = MockV1
    assert V1Query().to_dto(MagicMock()) == "v1"
    
    class V1Retrieval(SQLAlchemyRetrievalPersistence): entity_class = MockV1
    assert V1Retrieval().to_entity(MagicMock()) == "v1"
    
    op = MyOpPersistence()
    v1_inst = MockV1()
    # Force use of .dict() by deleting model_dump if any (though it won't have it)
    model = op.to_model(v1_inst)
    assert model.id == "v1"

@pytest.mark.asyncio
async def test_pydantic_v2_read_through():
    # Test model_validate (line 286)
    class MyEntity(BaseModel):
        id: str
        model_config = ConfigDict(from_attributes=True)
        
    class Retrieval(SQLAlchemyRetrievalPersistence): entity_class = MyEntity
    model = MagicMock()
    model.id = "pyd2"
    entity = Retrieval().to_entity(model)
    assert entity.id == "pyd2"
    
    # Query fallback (lines 329-330)
    class RawDTO:
        def __init__(self, id, **kwargs): self.id = id
    class Query(SQLAlchemyQueryPersistence): dto_class = RawDTO
    dto = Query().to_dto(model)
    assert dto.id == "pyd2"
