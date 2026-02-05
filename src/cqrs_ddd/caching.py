"""Caching decorators and utilities."""

from functools import wraps
from typing import Callable, Optional
import hashlib
import logging


from .protocols import CacheService

logger = logging.getLogger("cqrs_ddd")


def _default_key_builder(func: Callable, *args, **kwargs) -> str:
    """Generate a cache key based on function name and arguments."""
    key_parts = [func.__module__, func.__qualname__]
    if args:
        key_parts.append(str(args))
    if kwargs:
        key_parts.append(str(sorted(kwargs.items())))
    return hashlib.md5(":".join(key_parts).encode()).hexdigest()


def _resolve_cache_service(provider=None) -> Optional[CacheService]:
    """Resolve cache service from provider."""
    if provider:
        return provider()
    return None


# =============================================================================
# Function Decorators (General Purpose)
# =============================================================================


def cached(
    ttl: int = 60,
    key_builder: Callable = _default_key_builder,
    cache_service_provider: Optional[Callable[[], CacheService]] = None,
):
    """
    Decorator to cache method execution results using a pluggable cache service.

    If the cache service is unavailable or raises errors, the decorator fails gracefully
    and executes the decorated function as normal.

    Args:
        ttl: Time-to-live in seconds.
        key_builder: Function to generate cache key from arguments.
        cache_service_provider: Callable returning a CacheService instance.

    Usage:
        @cached(ttl=300)
        async def get_profile(profile_id: int):
            ...
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            service = _resolve_cache_service(cache_service_provider)
            if not service:
                return await func(*args, **kwargs)

            try:
                key = key_builder(func, *args, **kwargs)
                val = await service.get(key)
                if val:
                    return val
            except Exception:
                pass

            result = await func(*args, **kwargs)

            try:
                await service.set(key, result, ttl=ttl)
            except Exception:
                pass
            return result

        return wrapper

    return decorator


def cache_invalidate(
    key_builder: Optional[Callable] = None,
    cache_service_provider: Optional[Callable[[], CacheService]] = None,
):
    """
    Decorator to invalidate a specific cache key after successful execution.

    Args:
        key_builder: Function to generate the cache key to delete.
        cache_service_provider: Callable returning a CacheService instance.

    Usage:
        @cache_invalidate(key_builder=lambda f, uid, **kw: f"user:{uid}")
        async def update_user(user_id: int, data: dict):
            ...
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            service = _resolve_cache_service(cache_service_provider)
            if service and key_builder:
                try:
                    await service.delete(key_builder(func, *args, **kwargs))
                except Exception:
                    pass
            return result

        return wrapper

    return decorator
