from typing import List, Optional
from datetime import timezone
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from ..saga import SagaContext, SagaRepository


class SQLAlchemySagaRepository(SagaRepository):
    """SQLAlchemy implementation of SagaRepository for persistent storage."""

    def __init__(self, session: AsyncSession, table_name: str = "sagas"):
        self.session = session
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
        )

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
        }

        # Upsert logic (simplistic for generic sqlalchemy, specific dialects could use merge/upsert)
        stmt = sa.select(self.table.c.saga_id).where(
            self.table.c.saga_id == context.saga_id
        )
        result = await self.session.execute(stmt)
        exists = result.scalar() is not None

        if exists:
            update_stmt = (
                sa.update(self.table)
                .where(self.table.c.saga_id == context.saga_id)
                .values(**values)
            )
            await self.session.execute(update_stmt)
        else:
            insert_stmt = sa.insert(self.table).values(**values)
            await self.session.execute(insert_stmt)

    async def load(self, saga_id: str) -> Optional[SagaContext]:
        """Load saga context by ID."""
        stmt = sa.select(self.table).where(self.table.c.saga_id == saga_id)
        result = await self.session.execute(stmt)
        row = result.first()
        if not row:
            return None
        return self._map_to_context(row)

    async def find_by_correlation_id(
        self, correlation_id: str, saga_type: str
    ) -> Optional[SagaContext]:
        """Find saga context by correlation ID and type."""
        query = sa.select(self.table).where(
            self.table.c.correlation_id == correlation_id,
            self.table.c.saga_type == saga_type,
        )
        result = await self.session.execute(query)
        row = result.first()
        if not row:
            return None
        return self._map_to_context(row)

    async def find_stalled_sagas(self, limit: int = 10) -> List[SagaContext]:
        """Find sagas that are stalled."""
        query = (
            sa.select(self.table).where(self.table.c.is_stalled).limit(limit)
        )
        result = await self.session.execute(query)
        rows = result.all()
        return [self._map_to_context(row) for row in rows]

    def _map_to_context(self, row) -> SagaContext:
        # Convert row to dict
        data = dict(row._mapping)

        # Handle timezone conversion if necessary
        for col in ["created_at", "updated_at", "completed_at", "failed_at"]:
            if data.get(col) and data[col].tzinfo is None:
                data[col] = data[col].replace(tzinfo=timezone.utc)

        # Ensure new fields are present if not in DB (for migration compatibility)
        for field in [
            "compensations",
            "failed_compensations",
            "pending_commands",
            "processed_message_ids",
            "history",
            "state",
        ]:
            if data.get(field) is None:
                if field == "state":
                    data[field] = {}
                else:
                    data[field] = []

        return SagaContext(**data)
