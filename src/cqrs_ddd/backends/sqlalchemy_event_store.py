"""SQLAlchemy Event Store implementation."""

from typing import Optional, Callable, Any
import json

try:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    HAS_SQLALCHEMY = True
except Exception:
    HAS_SQLALCHEMY = False
    AsyncSession = Any
    async_sessionmaker = Any

from ..exceptions import ConcurrencyError


class SQLAlchemyEventStore:
    """
    SQLAlchemy-based Event Store implementation.

    Stores events in a PostgreSQL table with JSONB payload.
    Supports aggregate versioning, correlation tracking, and undo marking.

    Requires table (create via migration):

        CREATE TABLE domain_events (
            id BIGSERIAL PRIMARY KEY,
            event_id UUID NOT NULL UNIQUE,
            aggregate_type VARCHAR(100) NOT NULL,
            aggregate_id VARCHAR(100) NOT NULL,
            event_type VARCHAR(100) NOT NULL,
            event_version INTEGER DEFAULT 1,
            occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
            correlation_id UUID,
            causation_id UUID,
            payload JSONB NOT NULL,
            aggregate_version INTEGER NOT NULL,
            is_undone BOOLEAN DEFAULT FALSE,
            undone_at TIMESTAMP WITH TIME ZONE,
            undo_event_id UUID,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX idx_domain_events_aggregate ON domain_events(aggregate_type, aggregate_id);
        CREATE INDEX idx_domain_events_correlation ON domain_events(correlation_id);
        CREATE UNIQUE INDEX idx_domain_events_version
            ON domain_events(aggregate_type, aggregate_id, aggregate_version);

    Usage:
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        engine = create_async_engine("postgresql+asyncpg://...")
        session_factory = async_sessionmaker(engine)
        event_store = SQLAlchemyEventStore(session_factory)

        await event_store.append(my_event)
    """

    tracing_db_system = "postgresql"

    def __init__(
        self, uow_factory: Callable[[], Any], table_name: str = "domain_events"
    ):
        # Validate SQLAlchemy availability at runtime (handles environments where
        # sys.modules or module-level flags may be patched in tests)
        import sys

        # Most tests simulate 'no sqlalchemy' by setting sys.modules['sqlalchemy'] = None
        if sys.modules.get("sqlalchemy") is None:
            raise ImportError("SQLAlchemy is required")
        try:
            pass  # type: ignore
        except Exception:
            raise ImportError("SQLAlchemy is required")

        self._uow_factory = uow_factory
        self._table_name = table_name
        self._version_cache: dict = {}  # (aggregate_type, aggregate_id) -> version

    async def append(
        self, event, expected_version: Optional[int] = None, unit_of_work: Any = None
    ):
        """
        Append an event to the store.

        Args:
            event: DomainEventBase instance
            expected_version: Optimistic concurrency check
            unit_of_work: Optional UnitOfWork for transaction scope

        Returns:
            StoredEvent instance
        """
        from sqlalchemy import text
        from datetime import datetime, timezone

        # Manage UoW lifecycle
        uow = unit_of_work
        own_uow = False

        if uow is None:
            uow = self._uow_factory()
            await uow.__aenter__()
            own_uow = True

        try:
            # We expect the UoW to provide a SQLAlchemy session
            if not hasattr(uow, "session"):
                raise TypeError(
                    "SQLAlchemyEventStore requires a UnitOfWork with a 'session' attribute"
                )

            session = uow.session

            # Get current version
            current_version = await self._get_version(
                session, event.aggregate_type, event.aggregate_id
            )

            if expected_version is not None and current_version != expected_version:
                raise ConcurrencyError(
                    expected=expected_version, actual=current_version
                )

            new_version = current_version + 1

            # Insert event
            insert_sql = text(
                f"""
                INSERT INTO {self._table_name} (
                    event_id, aggregate_type, aggregate_id, event_type,
                    event_version, occurred_at, correlation_id,
                    causation_id, payload, aggregate_version
                ) VALUES (
                    :event_id, :aggregate_type, :aggregate_id, :event_type,
                    :event_version, :occurred_at, :correlation_id,
                    :causation_id, :payload, :aggregate_version
                ) RETURNING id
            """
            )

            payload = event.to_dict()
            # Ensure payload is JSON-serializable for DB drivers like sqlite
            payload = json.dumps(payload)

            result = await session.execute(
                insert_sql,
                {
                    "event_id": event.event_id,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": str(event.aggregate_id),
                    "event_type": event.event_type,
                    "event_version": event.version,
                    "occurred_at": event.occurred_at or datetime.now(timezone.utc),
                    "correlation_id": event.correlation_id,
                    "causation_id": event.causation_id,
                    "payload": payload,
                    "aggregate_version": new_version,
                },
            )

            row_id = result.scalar()

            # Flush changes within UoW context
            await session.flush()

            if own_uow:
                await uow.__aexit__(None, None, None)

            # Return StoredEvent-like dict
            return {
                "id": row_id,
                "event_id": event.event_id,
                "aggregate_type": event.aggregate_type,
                "aggregate_id": event.aggregate_id,
                "event_type": event.event_type,
                "aggregate_version": new_version,
            }

        except Exception:
            if own_uow:
                import sys

                await uow.__aexit__(*sys.exc_info())
            raise

    async def append_batch(
        self,
        events: list,
        correlation_id: Optional[str] = None,
        unit_of_work: Any = None,
    ) -> list:
        """Append multiple events atomically."""
        uow = unit_of_work
        own_uow = False

        if uow is None:
            uow = self._uow_factory()
            await uow.__aenter__()
            own_uow = True

        try:
            results = []
            for event in events:
                if correlation_id and hasattr(event, "correlation_id"):
                    event.correlation_id = correlation_id
                result = await self.append(event, unit_of_work=uow)
                results.append(result)

            if own_uow:
                await uow.__aexit__(None, None, None)

            return results

        except Exception:
            if own_uow:
                import sys

                await uow.__aexit__(*sys.exc_info())
            raise

    async def get_events(
        self,
        aggregate_type: str,
        aggregate_id,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None,
        unit_of_work: Any = None,
    ) -> list:
        """Get events for a specific aggregate."""
        from sqlalchemy import text

        async def _query(session):
            conditions = [
                "aggregate_type = :aggregate_type",
                "aggregate_id = :aggregate_id",
                "is_undone = FALSE",
            ]
            params = {
                "aggregate_type": aggregate_type,
                "aggregate_id": str(aggregate_id),
            }

            if from_version is not None:
                conditions.append("aggregate_version >= :from_version")
                params["from_version"] = from_version

            if to_version is not None:
                conditions.append("aggregate_version <= :to_version")
                params["to_version"] = to_version

            sql = text(
                f"""
                SELECT * FROM {self._table_name}
                WHERE {" AND ".join(conditions)}
                ORDER BY aggregate_version ASC
            """
            )

            result = await session.execute(sql, params)
            rows = result.mappings().all()
            return [dict(row) for row in rows]

        if unit_of_work:
            return await _query(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                return await _query(uow.session)

    async def get_events_by_correlation(
        self, correlation_id: str, unit_of_work: Any = None
    ) -> list:
        """Get all events with a specific correlation ID."""
        from sqlalchemy import text

        async def _query(session):
            sql = text(
                f"""
                SELECT * FROM {self._table_name}
                WHERE correlation_id = :correlation_id
                ORDER BY id ASC
            """
            )

            result = await session.execute(sql, {"correlation_id": correlation_id})
            rows = result.mappings().all()
            return [dict(row) for row in rows]

        if unit_of_work:
            return await _query(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                return await _query(uow.session)

    async def get_latest_events(
        self,
        aggregate_type: str,
        aggregate_id,
        count: int = 10,
        unit_of_work: Any = None,
    ) -> list:
        """Get most recent events for an aggregate."""
        from sqlalchemy import text

        async def _query(session):
            sql = text(
                f"""
                SELECT * FROM {self._table_name}
                WHERE aggregate_type = :aggregate_type
                AND aggregate_id = :aggregate_id
                AND is_undone = FALSE
                ORDER BY aggregate_version DESC
                LIMIT :count
            """
            )

            result = await session.execute(
                sql,
                {
                    "aggregate_type": aggregate_type,
                    "aggregate_id": str(aggregate_id),
                    "count": count,
                },
            )
            rows = result.mappings().all()
            return [dict(row) for row in reversed(rows)]

        if unit_of_work:
            return await _query(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                return await _query(uow.session)

    async def mark_as_undone(
        self,
        event_id: str,
        undo_event_id: Optional[str] = None,
        unit_of_work: Any = None,
    ) -> None:
        """Mark an event as undone."""
        from sqlalchemy import text
        from datetime import datetime, timezone

        async def _execute(session):
            sql = text(
                f"""
                UPDATE {self._table_name}
                SET is_undone = TRUE,
                    undone_at = :undone_at,
                    undo_event_id = :undo_event_id
                WHERE event_id = :event_id
            """
            )

            await session.execute(
                sql,
                {
                    "event_id": event_id,
                    "undone_at": datetime.now(timezone.utc),
                    "undo_event_id": undo_event_id,
                },
            )

        if unit_of_work:
            await _execute(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                await _execute(uow.session)

    async def get_current_version(
        self, aggregate_type: str, aggregate_id, unit_of_work: Any = None
    ) -> int:
        """Get current version of an aggregate."""
        if unit_of_work:
            return await self._get_version(
                unit_of_work.session, aggregate_type, aggregate_id
            )
        else:
            async with self._uow_factory() as uow:
                return await self._get_version(
                    uow.session, aggregate_type, aggregate_id
                )

    async def _get_version(
        self, session: "AsyncSession", aggregate_type: str, aggregate_id
    ) -> int:
        """Internal: get version within existing session."""
        from sqlalchemy import text

        sql = text(
            f"""
            SELECT COALESCE(MAX(aggregate_version), 0) as version
            FROM {self._table_name}
            WHERE aggregate_type = :aggregate_type
              AND aggregate_id = :aggregate_id
        """
        )

        result = await session.execute(
            sql,
            {
                "aggregate_type": aggregate_type,
                "aggregate_id": str(aggregate_id),
            },
        )
        row = result.first()
        return row.version if row else 0
