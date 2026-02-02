import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import importlib

# Pre-mock to allow imports
sys.modules["redis"] = MagicMock()
sys.modules["redis.asyncio"] = MagicMock()
sys.modules["redis.exceptions"] = MagicMock()

import cqrs_ddd.backends.redis_lock as redis_lock_mod  # noqa: E402
from cqrs_ddd.backends.redis_lock import RedisLockStrategy, InMemoryLockStrategy  # noqa: E402


@pytest.fixture
def mock_redis():
    mock = MagicMock()
    mock.eval = AsyncMock()
    mock.zrem = AsyncMock()
    mock.exists = AsyncMock(return_value=0)
    mock.get = AsyncMock()
    return mock


@pytest.mark.asyncio
async def test_redis_lock_init_dependency_fail(monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", False)
    with pytest.raises(ImportError, match="redis is required"):
        RedisLockStrategy(MagicMock())


@pytest.mark.asyncio
async def test_redis_lock_fallback_loading():
    # Simulate missing redis
    with patch.dict(
        sys.modules, {"redis": None, "redis.asyncio": None, "redis.exceptions": None}
    ):
        importlib.reload(redis_lock_mod)
        assert redis_lock_mod.HAS_REDIS is False
    # Restore
    sys.modules["redis"] = MagicMock()
    sys.modules["redis.asyncio"] = MagicMock()
    importlib.reload(redis_lock_mod)
    assert redis_lock_mod.HAS_REDIS is True


@pytest.mark.asyncio
async def test_redis_lock_acquire_success(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    strategy = RedisLockStrategy(mock_redis)

    # Mock successful eval for fair_acquire_script
    mock_redis.eval.return_value = 1

    token = await strategy.acquire("user", "123")
    assert isinstance(token, str)
    assert len(token) > 0
    mock_redis.eval.assert_called_once()

    # Test multi-resource acquisition
    mock_redis.eval.reset_mock()
    token = await strategy.acquire("user", ["123", "456"])
    assert mock_redis.eval.call_count == 2  # One for each ID


@pytest.mark.asyncio
async def test_redis_lock_acquire_timeout(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    # Slow down retries for test
    strategy = RedisLockStrategy(mock_redis, retry_interval=0.01, max_retries=2)

    # Mock eval returning 0 (still in queue)
    mock_redis.eval.return_value = 0

    with pytest.raises(TimeoutError, match="Could not acquire fair lock"):
        await strategy.acquire("user", "123", timeout=0.01)

    # Should call zrem on cleanup
    mock_redis.zrem.assert_called()


@pytest.mark.asyncio
async def test_redis_lock_release(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    strategy = RedisLockStrategy(mock_redis)

    mock_redis.eval.return_value = 1
    await strategy.release("user", "123", "token")
    mock_redis.eval.assert_called_once()

    # Test multi-release
    mock_redis.eval.reset_mock()
    await strategy.release("user", ["123", "456"], "token")
    mock_redis.eval.assert_called_once()  # Combined in one script call

    # Empty IDs
    mock_redis.eval.reset_mock()
    await strategy.release("user", [], "token")
    mock_redis.eval.assert_not_called()


@pytest.mark.asyncio
async def test_redis_lock_extend(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    strategy = RedisLockStrategy(mock_redis)

    mock_redis.eval.return_value = 1
    assert await strategy.extend("user", "123", "token") is True

    mock_redis.eval.return_value = 0
    assert await strategy.extend("user", "123", "token") is False


@pytest.mark.asyncio
async def test_redis_lock_metadata(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    strategy = RedisLockStrategy(mock_redis)

    mock_redis.exists.return_value = 1
    assert await strategy.is_locked("user", "123") is True

    mock_redis.get.return_value = b"token"
    assert await strategy.get_lock_owner("user", "123") == "token"

    mock_redis.get.return_value = None
    assert await strategy.get_lock_owner("user", "123") is None


@pytest.mark.asyncio
async def test_in_memory_lock_strategy():
    strategy = InMemoryLockStrategy(retry_interval=0.01, max_retries=5)

    # Success
    token = await strategy.acquire("user", "123")
    assert token in strategy._locks.values()

    # Multi-resource
    await strategy.acquire("post", ["1", "2"])
    assert "post:1" in strategy._locks
    assert "post:2" in strategy._locks

    # Conflict/Timeout
    with pytest.raises(TimeoutError):
        await strategy.acquire("user", "123", timeout=0.01)

    # Partial acquisition rollback
    # Lock "res:2" first (higher in sorted order)
    await strategy.acquire("res", "2")
    # Now try to lock ["1", "2"] - "1" will succeed, but "2" will fail
    # and "1" should be released.
    with pytest.raises(TimeoutError):
        await strategy.acquire("res", ["1", "2"])
    assert "res:1" not in strategy._locks

    # Release
    await strategy.release("user", "123", token)
    assert "user:123" not in strategy._locks

    # Clear
    strategy.clear()
    assert len(strategy._locks) == 0


@pytest.mark.asyncio
async def test_redis_lock_acquire_exception_cleanup(mock_redis, monkeypatch):
    monkeypatch.setattr(redis_lock_mod, "HAS_REDIS", True)
    strategy = RedisLockStrategy(mock_redis)

    # Mock exception during evaluation
    mock_redis.eval.side_effect = RuntimeError("Redis down")

    with pytest.raises(RuntimeError, match="Redis down"):
        await strategy.acquire("user", "123")

    # Should call release_all (via eval) even on unexpected error
    mock_redis.eval.assert_called()
