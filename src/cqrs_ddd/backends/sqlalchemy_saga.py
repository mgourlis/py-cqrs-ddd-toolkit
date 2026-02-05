from contextlib import asynccontextmanager
from typing import List, Optional, Any
from datetime import datetime, timezone
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession


from ..saga import SagaContext, SagaRepository


class SQLAlchemySagaRepository(SagaRepository):
    """
    SQLAlchemy implementation of SagaRepository for persistent storage.

    Supports both request-scoped (session-based) and singleton-scoped (factory-based) usage.
    """

    def __init__(
        self,
        session: Optional[AsyncSession] = None,
        session_factory: Optional[Any] = None,
        table_name: str = "sagas",
    ):
        self.session = session
        self.session_factory = session_factory
        self.metadata = sa.MetaData()

        # Define table schema
        self.table = sa.Table(
            table_name,
            self.metadata,
            sa.Column("saga_id", sa.String(64), primary_key=True),
            sa.Column("saga_type", sa.String(128)),
            sa.Column("correlation_id", sa.String(64), index=True),
            sa.Column("current_step", sa.String(128)),
            sa.Column("state", sa.JSON),
            sa.Column("history", sa.JSON),
            sa.Column("processed_message_ids", sa.JSON),
            sa.Column("created_at", sa.DateTime(timezone=True)),
            sa.Column("updated_at", sa.DateTime(timezone=True)),
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("error", sa.Text, nullable=True),
            sa.Column("is_completed", sa.Boolean, default=False),
            sa.Column("is_failed", sa.Boolean, default=False),
            sa.Column("is_stalled", sa.Boolean, default=False),
            sa.Column("compensations", sa.JSON),
            sa.Column("failed_compensations", sa.JSON),
            sa.Column("pending_commands", sa.JSON),
            # Suspension Support
            sa.Column("is_suspended", sa.Boolean, default=False),
            sa.Column("suspend_reason", sa.String(255), nullable=True),
            sa.Column("suspend_timeout_at", sa.DateTime(timezone=True), nullable=True),
        )

    @asynccontextmanager
    async def _get_session(self):
        """Helper to get a session, either from self.session or self.session_factory."""
        if self.session:
            yield self.session
        elif self.session_factory:
            async with self.session_factory() as session:
                yield session
        else:
            raise ValueError("Either session or session_factory must be provided")

    async def save(self, context: SagaContext) -> None:
        """Save or update saga context."""
        values = {
            "saga_id": context.saga_id,
            "saga_type": context.saga_type,
            "correlation_id": context.correlation_id,
            "current_step": context.current_step,
            "state": context.state,
            "history": context.history,
            "processed_message_ids": context.processed_message_ids,
            "created_at": context.created_at,
            "updated_at": context.updated_at,
            "completed_at": context.completed_at,
            "failed_at": context.failed_at,
            "error": context.error,
            "is_completed": context.is_completed,
            "is_failed": context.is_failed,
            "is_stalled": context.is_stalled,
            "compensations": context.compensations,
            "failed_compensations": context.failed_compensations,
            "pending_commands": context.pending_commands,
            "is_suspended": context.is_suspended,
            "suspend_reason": context.suspend_reason,
            "suspend_timeout_at": context.suspend_timeout_at,
        }

        async with self._get_session() as session:
            # Upsert logic
            stmt = sa.select(self.table.c.saga_id).where(
                self.table.c.saga_id == context.saga_id
            )
            result = await session.execute(stmt)
            exists = result.scalar() is not None

            if exists:
                update_stmt = (
                    sa.update(self.table)
                    .where(self.table.c.saga_id == context.saga_id)
                    .values(**values)
                )
                await session.execute(update_stmt)
            else:
                insert_stmt = sa.insert(self.table).values(**values)
                await session.execute(insert_stmt)

            if self.session_factory:
                await session.commit()

    async def load(self, saga_id: str) -> Optional[SagaContext]:
        """Load saga context by ID."""
        async with self._get_session() as session:
            stmt = sa.select(self.table).where(self.table.c.saga_id == saga_id)
            result = await session.execute(stmt)
            row = result.first()
            if not row:
                return None
            return self._map_to_context(row)

    async def find_by_correlation_id(
        self, correlation_id: str, saga_type: str
    ) -> Optional[SagaContext]:
        """Find saga context by correlation ID and type."""
        async with self._get_session() as session:
            query = sa.select(self.table).where(
                self.table.c.correlation_id == correlation_id,
                self.table.c.saga_type == saga_type,
            )
            result = await session.execute(query)
            row = result.first()
            if not row:
                return None
            return self._map_to_context(row)

    async def find_stalled_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are stalled."""
        async with self._get_session() as session:
            query = sa.select(self.table).where(self.table.c.is_stalled).limit(limit)
            result = await session.execute(query)
            rows = result.all()
            return [self._map_to_context(row) for row in rows]

    async def find_suspended_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are suspended."""
        async with self._get_session() as session:
            query = sa.select(self.table).where(self.table.c.is_suspended).limit(limit)
            result = await session.execute(query)
            rows = result.all()
            return [self._map_to_context(row) for row in rows]

    async def find_expired_suspended_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are suspended and have timed out."""
        async with self._get_session() as session:
            now = datetime.now(timezone.utc)
            query = (
                sa.select(self.table)
                .where(
                    self.table.c.is_suspended, self.table.c.suspend_timeout_at <= now
                )
                .limit(limit)
            )
            result = await session.execute(query)
            rows = result.all()
            return [self._map_to_context(row) for row in rows]

    def _map_to_context(self, row) -> SagaContext:
        # Convert row to dict
        data = dict(row._mapping)

        # Handle timezone conversion if necessary
        for col in [
            "created_at",
            "updated_at",
            "completed_at",
            "failed_at",
            "suspend_timeout_at",
        ]:
            if data.get(col) and data[col].tzinfo is None:
                data[col] = data[col].replace(tzinfo=timezone.utc)

        # Ensure new fields are present if not in DB (for migration compatibility)
        default_fields = {
            "compensations": [],
            "failed_compensations": [],
            "pending_commands": [],
            "processed_message_ids": [],
            "history": [],
            "state": {},
            "is_suspended": False,
            "suspend_reason": None,
            "suspend_timeout_at": None,
        }

        for field_name, default_value in default_fields.items():
            if data.get(field_name) is None:
                data[field_name] = default_value

        return SagaContext(**data)
