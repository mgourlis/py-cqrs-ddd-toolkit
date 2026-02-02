"""Test configuration and global fixtures.

This file sets up adapters/converters that prevent DeprecationWarnings from
Python's sqlite3 when using datetime/timestamp in aiomysql/aiosqlite tests.
"""

import sqlite3
from datetime import datetime


# SQLite datetime adapter: serialize as ISO format string
def _adapt_datetime(dt: datetime) -> str:
    return dt.isoformat(sep=" ")


# Converter for timestamp columns -> datetime
def _convert_timestamp(b: bytes):
    # Return string (let SQLAlchemy perform any further parsing)
    try:
        return b.decode()
    except Exception:
        return b


# Register adapters and converters at test session start
sqlite3.register_adapter(datetime, _adapt_datetime)
sqlite3.register_converter("timestamp", _convert_timestamp)
sqlite3.register_converter("TIMESTAMP", _convert_timestamp)
