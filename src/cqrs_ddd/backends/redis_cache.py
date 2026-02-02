"""Redis cache backend implementation."""

from typing import Any, Optional, List
import json

try:
    from redis.asyncio import Redis

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False
    Redis = Any


class RedisCacheService:
    """
    Redis-based cache implementation.

    Implements the CacheService protocol with Redis as the backend.
    Supports TTL, batch operations, and JSON serialization.

    Usage:
        from redis.asyncio import Redis

        redis = Redis.from_url("redis://localhost:6379")
        cache = RedisCacheService(redis, prefix="myapp")

        await cache.set("user:123", {"name": "Alice"}, ttl=300)
        user = await cache.get("user:123")
    """

    def __init__(self, redis: "Redis", prefix: str = "cqrs"):
        """
        Initialize Redis cache.

        Args:
            redis: Async Redis client instance
            prefix: Key prefix for namespacing (default: "cqrs")
        """
        if not HAS_REDIS:
            raise ImportError(
                "redis is required. Install with: pip install py-cqrs-ddd-toolkit[redis]"
            )

        self.redis = redis
        self.prefix = prefix
        self._import_dependencies()

    def _import_dependencies(self):
        """Lazy import of serialization dependencies."""
        global pickle, zlib
        import pickle
        import zlib

    def _key(self, key: str) -> str:
        """Generate prefixed key."""
        return f"{self.prefix}:{key}"

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Deserializes using pickle and decompresses using zlib.
        """
        data = await self.redis.get(self._key(key))
        if data:
            try:
                # Try new format (compressed pickle)
                decompressed = zlib.decompress(data)
                return pickle.loads(decompressed)
            except (zlib.error, pickle.UnpicklingError):
                # Fallback for plain JSON (migration path)
                try:
                    return json.loads(data)
                except Exception:
                    return None
        return None

    async def set(self, key: str, value: Any, ttl: int = None) -> None:
        """
        Set value in cache with optional TTL (seconds).

        Serializes using pickle and compresses using zlib.
        """
        serialized = zlib.compress(pickle.dumps(value))
        if ttl:
            await self.redis.set(self._key(key), serialized, ex=ttl)
        else:
            await self.redis.set(self._key(key), serialized)

    async def delete(self, key: str) -> None:
        """Delete value from cache."""
        await self.redis.delete(self._key(key))

    async def delete_batch(self, keys: List[str]) -> None:
        """Delete multiple values from cache."""
        if keys:
            prefixed_keys = [self._key(k) for k in keys]
            await self.redis.delete(*prefixed_keys)

    async def get_batch(self, keys: List[str]) -> List[Optional[Any]]:
        """Get multiple values from cache."""
        if not keys:
            return []

        prefixed_keys = [self._key(k) for k in keys]
        values = await self.redis.mget(prefixed_keys)

        results = []
        for v in values:
            if v:
                try:
                    decompressed = zlib.decompress(v)
                    results.append(pickle.loads(decompressed))
                except Exception:
                    results.append(None)
            else:
                results.append(None)
        return results

    async def set_batch(self, entries: List[dict], ttl: int = None) -> None:
        """
        Set multiple values in cache.

        Each entry should have 'cache_key' and 'value' keys.
        """
        if not entries:
            return

        async with self.redis.pipeline() as pipe:
            for entry in entries:
                key = self._key(entry["cache_key"])
                value = entry["value"]
                serialized = zlib.compress(pickle.dumps(value))

                if ttl:
                    pipe.set(key, serialized, ex=ttl)
                else:
                    pipe.set(key, serialized)

            await pipe.execute()

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        return await self.redis.exists(self._key(key)) > 0

    async def expire(self, key: str, ttl: int) -> bool:
        """Set TTL on existing key."""
        return await self.redis.expire(self._key(key), ttl)

    async def ttl(self, key: str) -> int:
        """Get remaining TTL for a key (-1 if no TTL, -2 if not exists)."""
        return await self.redis.ttl(self._key(key))

    def generate_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate cache key for entity caching."""
        return f"{app}:{entity}:{id}"

    def generate_query_cache_key(self, app: str, entity: str, id: Any) -> str:
        """Generate cache key for query result caching."""
        return f"{app}:{entity}:query:{id}"

    async def flush_prefix(self, pattern: str = "*") -> int:
        """
        Delete all keys matching pattern under this prefix.

        Use with caution in production!

        Args:
            pattern: Glob pattern (default: "*" for all keys)

        Returns:
            Number of keys deleted
        """
        full_pattern = f"{self.prefix}:{pattern}"
        keys = []

        async for key in self.redis.scan_iter(match=full_pattern):
            keys.append(key)

        if keys:
            return await self.redis.delete(*keys)
        return 0
