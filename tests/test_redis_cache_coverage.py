
import pytest
import json
import zlib
import pickle
import sys
import importlib
from unittest.mock import MagicMock, AsyncMock, patch, ANY

# Pre-mock to allow imports
sys.modules["redis"] = MagicMock()
sys.modules["redis.asyncio"] = MagicMock()

import cqrs_ddd.backends.redis_cache as redis_cache_mod
from cqrs_ddd.backends.redis_cache import RedisCacheService

@pytest.fixture
def mock_redis():
    mock = MagicMock()
    mock.get = AsyncMock()
    mock.set = AsyncMock()
    mock.delete = AsyncMock()
    mock.mget = AsyncMock()
    mock.exists = AsyncMock(return_value=0)
    mock.expire = AsyncMock(return_value=True)
    mock.ttl = AsyncMock(return_value=-1)
    
    # Mock pipeline
    mock_pipe = AsyncMock()
    mock_pipe.__aenter__.return_value = mock_pipe
    mock_pipe.set = MagicMock()
    mock_pipe.execute = AsyncMock()
    mock.pipeline.return_value = mock_pipe
    
    # Mock scan_iter
    async def mock_scan_iter(match=None):
        yield b"cqrs:key1"
        yield b"cqrs:key2"
    mock.scan_iter = mock_scan_iter
    
    return mock

@pytest.mark.asyncio
async def test_redis_cache_fallback_loading():
    # Simulate missing redis
    with patch.dict(sys.modules, {"redis": None, "redis.asyncio": None}):
        importlib.reload(redis_cache_mod)
        assert redis_cache_mod.HAS_REDIS is False
    # Restore
    sys.modules["redis"] = MagicMock()
    sys.modules["redis.asyncio"] = MagicMock()
    importlib.reload(redis_cache_mod)
    assert redis_cache_mod.HAS_REDIS is True

@pytest.mark.asyncio
async def test_redis_cache_init_dependency_fail(monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", False)
    with pytest.raises(ImportError, match="redis is required"):
        RedisCacheService(MagicMock())

@pytest.mark.asyncio
async def test_redis_cache_crud(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    
    # Set
    await cache.set("k1", "v1", ttl=10)
    mock_redis.set.assert_called_once()
    args, kwargs = mock_redis.set.call_args
    assert args[0] == "test:k1"
    assert kwargs["ex"] == 10
    
    # Get success (compressed pickle)
    val = "v1"
    data = zlib.compress(pickle.dumps(val))
    mock_redis.get.return_value = data
    assert await cache.get("k1") == "v1"
    
    # Get success (json fallback)
    mock_redis.get.return_value = json.dumps({"a": 1}).encode()
    assert await cache.get("k1") == {"a": 1}
    
    # Get failure (corrupt)
    mock_redis.get.return_value = b"corrupt"
    assert await cache.get("k1") is None
    
    # Get none
    mock_redis.get.return_value = None
    assert await cache.get("k1") is None
    
    # Delete
    await cache.delete("k1")
    mock_redis.delete.assert_called_with("test:k1")

@pytest.mark.asyncio
async def test_redis_cache_batch(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    
    # set_batch
    entries = [
        {"cache_key": "k1", "value": "v1"},
        {"cache_key": "k2", "value": "v2"}
    ]
    await cache.set_batch(entries, ttl=60)
    pipe = mock_redis.pipeline.return_value
    assert pipe.set.call_count == 2
    pipe.execute.assert_called_once()
    
    # Empty set_batch
    pipe.set.reset_mock()
    await cache.set_batch([])
    pipe.set.assert_not_called()
    
    # get_batch
    mock_redis.mget.return_value = [
        zlib.compress(pickle.dumps("v1")),
        None,
        b"corrupt"
    ]
    res = await cache.get_batch(["k1", "k2", "k3"])
    assert res == ["v1", None, None]
    
    # Empty get_batch
    assert await cache.get_batch([]) == []
    
    # delete_batch
    await cache.delete_batch(["k1", "k2"])
    mock_redis.delete.assert_called_with("test:k1", "test:k2")
    
    # Empty delete_batch
    mock_redis.delete.reset_mock()
    await cache.delete_batch([])
    mock_redis.delete.assert_not_called()

@pytest.mark.asyncio
async def test_redis_cache_metadata(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    
    # Exists
    mock_redis.exists.return_value = 1
    assert await cache.exists("k") is True
    mock_redis.exists.return_value = 0
    assert await cache.exists("k") is False
    
    # Expire
    await cache.expire("k", 100)
    mock_redis.expire.assert_called_with("test:k", 100)
    
    # TTL
    mock_redis.ttl.return_value = 50
    assert await cache.ttl("k") == 50
    
    # Key generators
    assert cache.generate_cache_key("a", "e", "i") == "a:e:i"
    assert cache.generate_query_cache_key("a", "e", "i") == "a:e:query:i"

@pytest.mark.asyncio
async def test_redis_cache_flush(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    
    # Flush with keys
    mock_redis.delete.return_value = 2
    count = await cache.flush_prefix()
    assert count == 2
    mock_redis.delete.assert_called()
    
    # Flush with no keys
    async def mock_scan_iter_empty(match=None):
        if False: yield b""
    mock_redis.scan_iter = mock_scan_iter_empty
    
    count = await cache.flush_prefix()
    assert count == 0

@pytest.mark.asyncio
async def test_redis_cache_set_no_ttl(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    await cache.set("k", "v")
    mock_redis.set.assert_called_with("test:k", ANY)
    
@pytest.mark.asyncio
async def test_redis_cache_set_batch_no_ttl(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_cache_mod, "HAS_REDIS", True)
    cache = RedisCacheService(mock_redis, prefix="test")
    entries = [{"cache_key": "k1", "value": "v1"}]
    await cache.set_batch(entries)
    pipe = mock_redis.pipeline.return_value
    pipe.set.assert_called_with("test:k1", ANY)
