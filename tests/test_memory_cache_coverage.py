
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import patch
from cqrs_ddd.backends.memory_cache import MemoryCacheService

@pytest.mark.asyncio
async def test_memory_cache_crud():
    cache = MemoryCacheService()
    
    # Set/Get
    await cache.set("k1", "v1")
    assert await cache.get("k1") == "v1"
    assert cache.size() == 1
    
    # Update (clear expiry)
    await cache.set("k1", "v2")
    assert await cache.get("k1") == "v2"
    
    # Delete
    await cache.delete("k1")
    assert await cache.get("k1") is None
    assert cache.size() == 0

@pytest.mark.asyncio
async def test_memory_cache_ttl():
    cache = MemoryCacheService()
    
    # Set with TTL
    await cache.set("k1", "v1", ttl=1)
    assert await cache.get("k1") == "v1"
    
    # Mock time to future
    future = datetime.now() + timedelta(seconds=2)
    with patch("cqrs_ddd.backends.memory_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = future
        assert await cache.get("k1") is None
        assert cache.size() == 0
    
    # Set with TTL and then overwrite without TTL
    await cache.set("k2", "v2", ttl=1)
    await cache.set("k2", "v2") # Should clear expiry
    assert "k2" not in cache._expiry

@pytest.mark.asyncio
async def test_memory_cache_batch():
    cache = MemoryCacheService()
    
    entries = [
        {"cache_key": "k1", "value": "v1"},
        {"cache_key": "k2", "value": "v2"}
    ]
    await cache.set_batch(entries)
    assert await cache.get_batch(["k1", "k2"]) == ["v1", "v2"]
    
    await cache.delete_batch(["k1", "k2"])
    assert cache.size() == 0
    assert await cache.get_batch(["k1", "k2"]) == [None, None]

@pytest.mark.asyncio
async def test_memory_cache_utils():
    cache = MemoryCacheService()
    
    assert cache.generate_cache_key("a", "e", "i") == "a:e:i"
    assert cache.generate_query_cache_key("a", "e", "i") == "a:e:query:i"
    
    await cache.set("k", "v")
    cache.clear()
    assert cache.size() == 0
