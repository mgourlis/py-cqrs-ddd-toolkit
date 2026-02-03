"""SQLAlchemy backend - Unit of Work and persistence implementations."""

from typing import Optional, Callable, Any, List, Type, TYPE_CHECKING, TypeVar, Generic

try:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, AsyncEngine
    from sqlalchemy import select, DateTime, String, Integer, Boolean
    from sqlalchemy.orm import Mapped, mapped_column, declarative_mixin
    from sqlalchemy.sql import func
    from datetime import datetime

    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    AsyncSession = Any
    async_sessionmaker = Any
    AsyncEngine = Any
    Mapped = Any
    mapped_column = Any

    def declarative_mixin(x):
        return x


from ..persistence_dispatcher import (
    OperationPersistence,
    QueryPersistence,
    RetrievalPersistence,
)
from ..exceptions import ConcurrencyError

if TYPE_CHECKING:
    pass


# =============================================================================
# Unit of Work
# =============================================================================


class SQLAlchemyUnitOfWork:
    """
    SQLAlchemy-based Unit of Work implementation.

    Implements the UnitOfWork protocol for transaction management.

    Usage:
        engine = create_async_engine("postgresql+asyncpg://...")
        session_factory = async_sessionmaker(engine)
        uow_factory = lambda: SQLAlchemyUnitOfWork(session_factory)

        async with uow_factory() as uow:
            # uow.session is the AsyncSession
            result = await uow.session.execute(...)
            # Auto-commits on exit, auto-rollbacks on exception
    """

    def __init__(
        self,
        session_factory: "async_sessionmaker[AsyncSession]" = None,
        session: "AsyncSession" = None,
    ):
        """
        Initialize Unit of Work.

        Args:
            session_factory: Factory to create new sessions (preferred)
            session: Existing session to wrap (for nested UoW)
        """
        if not HAS_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required for SQLAlchemyUnitOfWork. "
                "Install with: pip install py-cqrs-ddd-toolkit[sqlalchemy]"
            )

        self._session_factory = session_factory
        self._session = session
        self._owns_session = session is None

    @property
    def session(self) -> "AsyncSession":
        """Get the current session."""
        if self._session is None:
            raise RuntimeError("UnitOfWork not started. Use 'async with uow:'")
        return self._session

    async def __aenter__(self) -> "SQLAlchemyUnitOfWork":
        """Start the unit of work scope."""
        if self._owns_session:
            if self._session_factory is None:
                raise RuntimeError("No session factory provided")
            self._session = self._session_factory()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """End the unit of work scope."""
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        finally:
            if self._owns_session and self._session:
                await self._session.close()
                self._session = None
        return False

    async def commit(self) -> None:
        """Explicitly commit the transaction."""
        if self._session:
            await self._session.commit()

    async def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        if self._session:
            await self._session.rollback()

    async def flush(self) -> None:
        """Flush pending changes without committing."""
        if self._session:
            await self._session.flush()


def create_uow_factory(
    engine: "AsyncEngine" = None,
    session_factory: "async_sessionmaker[AsyncSession]" = None,
) -> Callable[[], SQLAlchemyUnitOfWork]:
    """
    Create a UnitOfWork factory function.

    Args:
        engine: SQLAlchemy async engine (will create session factory)
        session_factory: Pre-configured session factory

    Returns:
        Factory function that creates new UnitOfWork instances

    Usage:
        uow_factory = create_uow_factory(engine)
        dispatcher = PersistenceDispatcher(uow_factory)
    """
    if not HAS_SQLALCHEMY:
        raise ImportError("SQLAlchemy is required")

    if session_factory is None and engine is not None:
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )

    if session_factory is None:
        raise ValueError("Either engine or session_factory must be provided")

    def factory() -> SQLAlchemyUnitOfWork:
        return SQLAlchemyUnitOfWork(session_factory=session_factory)

    return factory


# =============================================================================
# SQLAlchemy Mixins
# =============================================================================

if HAS_SQLALCHEMY:

    @declarative_mixin
    class SQLAlchemyEntityMixin:
        """
        Mixin for SQLAlchemy models implementing standard DDD Entity traits.

        Includes:
        - id: Primary Key (Generic/Overridable, usually String/UUID)
        - version: Optimistic concurrency version
        - created_at, updated_at, created_by
        - is_deleted, deleted_at (Soft Delete)

        Usage:
            class UserModel(Base, SQLAlchemyEntityMixin):
                __tablename__ = "users"
                # id is provided by mixin, can be overridden if needed
                name: Mapped[str] = mapped_column(String)
        """

        # Primary Key - Default to String (UUID compatible), overridable in subclass
        id: Mapped[str] = mapped_column(String, primary_key=True)

        # Concurrency
        version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

        # Audit
        created_at: Mapped[datetime] = mapped_column(
            DateTime(timezone=True), default=func.now(), nullable=False
        )
        updated_at: Mapped[Optional[datetime]] = mapped_column(
            DateTime(timezone=True), onupdate=func.now(), nullable=True
        )
        created_by: Mapped[Optional[str]] = mapped_column(String, nullable=True)

        # Soft Delete
        is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
        deleted_at: Mapped[Optional[datetime]] = mapped_column(
            DateTime(timezone=True), nullable=True
        )


# =============================================================================
# Base SQLAlchemy Persistence (for new dispatcher pattern)
# =============================================================================

T = TypeVar("T")


class SQLAlchemyOperationPersistence(OperationPersistence[T], Generic[T]):
    """
    Base class for SQLAlchemy-based operation persistence.

    Supports automatic conversion for:
    - Standard Python objects (via __dict__)
    - Pydantic models (via model_dump)
    - Dataclasses
    """

    model_class: Type = None  # The SQLAlchemy model class

    def to_model(self, entity: Any) -> Any:
        """
        Convert domain entity to SQLAlchemy model.
        """
        if hasattr(entity, "model_dump"):
            # Pydantic v2
            data = entity.model_dump()
        elif hasattr(entity, "dict"):
            # Pydantic v1
            data = entity.dict()
        elif hasattr(entity, "__dict__"):
            # Standard object / Dataclass
            data = {k: v for k, v in entity.__dict__.items() if not k.startswith("_")}
        else:
            return entity

        # Filter fields to only those present in the SQLAlchemy model
        # This prevents errors when domain has extra fields not in DB
        if self.model_class and hasattr(self.model_class, "__table__"):
            columns = self.model_class.__table__.columns.keys()
            data = {k: v for k, v in data.items() if k in columns}

        return self.model_class(**data)

    async def check_version(
        self, session: "AsyncSession", entity: Any, model_class: Type = None
    ) -> None:
        """Helper: Check optimistic concurrency version."""
        model_class = model_class or self.model_class
        if not model_class:
            return

        if not hasattr(entity, "id") or not hasattr(entity, "version"):
            return

        stmt = select(model_class).where(model_class.id == entity.id)
        result = await session.execute(stmt)
        db_entity = result.scalar_one_or_none()

        if db_entity and hasattr(db_entity, "version"):
            expected_version = entity.version - 1
            if db_entity.version != expected_version:
                raise ConcurrencyError(
                    expected=expected_version, actual=db_entity.version
                )


class SQLAlchemyRetrievalPersistence(RetrievalPersistence[T], Generic[T]):
    """
    Base class for SQLAlchemy-based domain entity retrieval.

    Supports automatic conversion if 'entity_class' is set.
    """

    model_class: Type = None
    entity_class: Type = None  # Set this for auto-conversion

    def to_entity(self, model: Any) -> Any:
        """
        Convert SQLAlchemy model to domain entity.
        """
        if not self.entity_class:
            raise NotImplementedError(
                "Override to_entity() or set 'entity_class' class attribute."
            )

        # Pydantic v2 model_validate (from attributes)
        if hasattr(self.entity_class, "model_validate"):
            # SQLAlchemy models are essentially objects with attributes,
            # so model_validate(obj, from_attributes=True) is ideal.
            return self.entity_class.model_validate(model, from_attributes=True)

        # Pydantic v1 from_orm
        if hasattr(self.entity_class, "from_orm"):
            return self.entity_class.from_orm(model)

        # Fallback: Dictionary unpacking
        # WARN: This might fail if model has relationships that cause recursion
        # or if entity_class expects arguments not in model.__dict__
        data = {k: v for k, v in model.__dict__.items() if not k.startswith("_")}
        return self.entity_class(**data)

    async def retrieve(self, entity_ids: List[Any], unit_of_work: Any) -> List[Any]:
        """Retrieve domain entities by IDs."""
        session = unit_of_work.session
        stmt = select(self.model_class).where(self.model_class.id.in_(entity_ids))
        result = await session.execute(stmt)
        models = result.scalars().all()
        return [self.to_entity(m) for m in models]


class SQLAlchemyQueryPersistence(QueryPersistence[T], Generic[T]):
    """
    Base class for SQLAlchemy-based query/read model persistence.
    """

    model_class: Type = None
    dto_class: Type = None  # Set this for auto-conversion

    def to_dto(self, model: Any) -> Any:
        """
        Convert SQLAlchemy model to DTO.
        """
        if not self.dto_class:
            raise NotImplementedError(
                "Override to_dto() or set 'dto_class' class attribute."
            )

        if hasattr(self.dto_class, "model_validate"):
            return self.dto_class.model_validate(model, from_attributes=True)

        if hasattr(self.dto_class, "from_orm"):
            return self.dto_class.from_orm(model)

        data = {k: v for k, v in model.__dict__.items() if not k.startswith("_")}
        return self.dto_class(**data)

    async def fetch(self, entity_ids: List[Any], unit_of_work: Any) -> List[Any]:
        """Fetch DTOs by entity IDs."""
        session = unit_of_work.session
        stmt = select(self.model_class).where(self.model_class.id.in_(entity_ids))
        result = await session.execute(stmt)
        models = result.scalars().all()
        return [self.to_dto(m) for m in models]
