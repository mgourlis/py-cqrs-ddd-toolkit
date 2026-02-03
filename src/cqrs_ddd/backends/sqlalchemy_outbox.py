"""SQLAlchemy backend for Transactional Outbox."""

from typing import List, Callable, Any, Type
import uuid
import json
from datetime import datetime, timezone

try:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    HAS_SQLALCHEMY = True
except ImportError:
    HAS_SQLALCHEMY = False
    AsyncSession = Any
    async_sessionmaker = Any

from ..outbox import OutboxStorage, OutboxMessage
from ..core import AbstractOutboxMessage


class SQLAlchemyOutboxStorage(OutboxStorage):
    """
    SQLAlchemy-based Outbox Storage.

    Persists messages to 'outbox_messages' table.

    Requires table:
        CREATE TABLE outbox_messages (
            id UUID PRIMARY KEY,
            occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
            type VARCHAR(100) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            payload JSONB NOT NULL,
            correlation_id VARCHAR(255),
            status VARCHAR(50) DEFAULT 'pending',
            retries INTEGER DEFAULT 0,
            error TEXT,
            processed_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX idx_outbox_pending ON outbox_messages(status, occurred_at)
        WHERE status IN ('pending', 'failed');
    """

    def __init__(
        self,
        uow_factory: Callable[[], Any],
        table_name: str = "outbox_messages",
        message_class: Type[AbstractOutboxMessage] = OutboxMessage,
    ):
        if not HAS_SQLALCHEMY:
            raise ImportError(
                "SQLAlchemy is required. Install with: pip install py-cqrs-ddd-toolkit[sqlalchemy]"
            )

        self._uow_factory = uow_factory
        self._table_name = table_name
        self._message_class = message_class

        # Dialect flags (lazy-initialized on first query)
        self._dialect_checked = False
        self._dialect_name = ""
        self._supports_skip_locked = False
        self._needs_uuid_str_conversion = False

    def _check_dialect(self, session: "AsyncSession") -> None:
        """Check dialect capabilities once and cache flags."""
        if self._dialect_checked:
            return

        try:
            dialect = getattr(session.bind, "dialect", None)
            if dialect:
                self._dialect_name = getattr(dialect, "name", "")

                # PostgreSQL supports SKIP LOCKED
                self._supports_skip_locked = self._dialect_name == "postgresql"

                # SQLite requires UUID -> str conversion
                self._needs_uuid_str_conversion = self._dialect_name == "sqlite"

            self._dialect_checked = True
        except Exception:
            # Fallback: assume no special features
            self._dialect_checked = True
            self._dialect_name = ""
            self._supports_skip_locked = False
            self._needs_uuid_str_conversion = True  # safer default

    async def save(
        self, message: AbstractOutboxMessage, unit_of_work: Any = None
    ) -> None:
        """Save a new message to the outbox."""
        from sqlalchemy import text

        async def _save(session):
            self._check_dialect(session)

            sql = text(
                f"""
                INSERT INTO {self._table_name} (
                    id, occurred_at, type, topic, payload,
                    correlation_id, status, retries, error
                ) VALUES (
                    :id, :occurred_at, :type, :topic, :payload,
                    :correlation_id, :status, :retries, :error
                )
            """
            )

            # Use flag for UUID conversion
            msg_id = str(message.id) if self._needs_uuid_str_conversion else message.id  # type: ignore

            await session.execute(
                sql,
                {
                    "id": msg_id,
                    "occurred_at": message.occurred_at,  # type: ignore
                    "type": message.type,  # type: ignore
                    "topic": message.topic,  # type: ignore
                    "payload": json.dumps(message.payload),  # type: ignore
                    "correlation_id": message.correlation_id,  # type: ignore
                    "status": getattr(message, "status", "pending"),
                    "retries": getattr(message, "retries", 0),
                    "error": getattr(message, "error", None),
                },
            )
            # If in UoW, changes are flushed but not committed
            # If standalone, we commit

        if unit_of_work:
            await _save(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                await _save(uow.session)

    async def get_pending(
        self, batch_size: int = 50, unit_of_work: Any = None
    ) -> List[AbstractOutboxMessage]:
        """Get pending messages ordered by occurrence time."""
        from sqlalchemy import text

        async def _query(session):
            self._check_dialect(session)

            # Build SQL with locking clause based on dialect
            base_sql = f"""
                SELECT * FROM {self._table_name}
                WHERE status = 'pending'
                   OR (status = 'failed' AND retries < 5)
                ORDER BY occurred_at ASC
                LIMIT :batch_size
            """

            lock_clause = (
                " FOR UPDATE SKIP LOCKED" if self._supports_skip_locked else ""
            )
            sql = text(base_sql + lock_clause)

            result = await session.execute(sql, {"batch_size": batch_size})
            rows = result.mappings().all()

            messages = []
            for row in rows:
                # Convert row mapping to dict and handle binary/string differences
                data = dict(row)
                if isinstance(data.get("payload"), str):
                    data["payload"] = json.loads(data["payload"])

                # Normalize id back to UUID for message class
                try:
                    import uuid as _uuid

                    if isinstance(data.get("id"), str):
                        data["id"] = _uuid.UUID(data["id"])
                except Exception:
                    pass

                # Remove DB-only fields not in OutboxMessage
                data.pop("created_at", None)

                # Automap to message class
                if hasattr(self._message_class, "model_validate"):
                    # Pydantic support
                    messages.append(self._message_class.model_validate(data))
                elif hasattr(self._message_class, "from_dict"):
                    messages.append(self._message_class.from_dict(data))  # type: ignore
                else:
                    # Generic mapping (dataclasses, etc.)
                    messages.append(self._message_class(**data))  # type: ignore
            return messages

        if unit_of_work:
            return await _query(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                return await _query(uow.session)

    async def mark_published(
        self, message_id: uuid.UUID, unit_of_work: Any = None
    ) -> None:
        """Mark message as successfully published."""
        from sqlalchemy import text

        async def _execute(session):
            self._check_dialect(session)

            sql = text(
                f"""
                UPDATE {self._table_name}
                SET status = 'published',
                    processed_at = :now
                WHERE id = :id
            """
            )

            msg_id = str(message_id) if self._needs_uuid_str_conversion else message_id

            await session.execute(
                sql, {"id": msg_id, "now": datetime.now(timezone.utc)}
            )

        if unit_of_work:
            await _execute(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                await _execute(uow.session)

    async def mark_failed(
        self, message_id: uuid.UUID, error: str, unit_of_work: Any = None
    ) -> None:
        """Mark message as failed and increment retries."""
        from sqlalchemy import text

        async def _execute(session):
            self._check_dialect(session)

            sql = text(
                f"""
                UPDATE {self._table_name}
                SET status = 'failed',
                    retries = retries + 1,
                    error = :error,
                    processed_at = :now
                WHERE id = :id
            """
            )

            msg_id = str(message_id) if self._needs_uuid_str_conversion else message_id

            await session.execute(
                sql,
                {"id": msg_id, "error": str(error), "now": datetime.now(timezone.utc)},
            )

        if unit_of_work:
            await _execute(unit_of_work.session)
        else:
            async with self._uow_factory() as uow:
                await _execute(uow.session)
