"""In-memory cache backend for testing and development."""

from typing import Any, Optional, List, Dict
from datetime import datetime, timedelta


class MemoryCacheService:
    """
    In-memory cache implementation for testing and development.

    NOT for production use - no TTL expiration, no persistence.
    Implements the CacheService protocol.

    Usage:
        cache = MemoryCacheService()
        await cache.set("key", {"data": "value"}, ttl=300)
        data = await cache.get("key")
    """

    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._expiry: Dict[str, datetime] = {}

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key in self._expiry:
            if datetime.now() > self._expiry[key]:
                del self._cache[key]
                del self._expiry[key]
                return None
        return self._cache.get(key)

    async def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Set value in cache with optional TTL (seconds)."""
        self._cache[key] = value
        if ttl:
            self._expiry[key] = datetime.now() + timedelta(seconds=ttl)
        elif key in self._expiry:
            del self._expiry[key]

    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        self._cache.pop(key, None)
        self._expiry.pop(key, None)

    async def delete_batch(self, keys: List[str]) -> None:
        """Delete multiple values from cache."""
        for key in keys:
            self._cache.pop(key, None)
            self._expiry.pop(key, None)

    async def get_batch(self, keys: List[str]) -> List[Optional[Any]]:
        """Get multiple values from cache."""
        return [await self.get(key) for key in keys]

    async def set_batch(self, entries: List[dict], ttl: int = None) -> None:
        """
        Set multiple values in cache.

        Each entry should have 'cache_key' and 'value' keys.
        """
        for entry in entries:
            await self.set(entry["cache_key"], entry["value"], ttl)

    def generate_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate cache key for entity caching."""
        return f"{app}:{entity}:{id}"

    def generate_query_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate cache key for query result caching."""
        return f"{app}:{entity}:query:{id}"

    def clear(self) -> None:
        """Clear all cache entries (for testing)."""
        self._cache.clear()
        self._expiry.clear()

    def size(self) -> int:
        """Get number of cached items."""
        return len(self._cache)
