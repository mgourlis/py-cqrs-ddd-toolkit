import pytest
from unittest.mock import MagicMock, AsyncMock
from cqrs_ddd.caching import cached, cache_invalidate, _default_key_builder

# --- Helpers ---


class MockCacheService:
    def __init__(self):
        self.store = {}
        self.get_called = 0
        self.set_called = 0
        self.delete_called = 0

    async def get(self, key):
        self.get_called += 1
        return self.store.get(key)

    async def set(self, key, value, ttl=None):
        self.set_called += 1
        self.store[key] = value

    async def delete(self, key):
        self.delete_called += 1
        if key in self.store:
            del self.store[key]


# We need a provider that returns our mock
mock_service = MockCacheService()


def get_mock_service():
    return mock_service


@pytest.fixture(autouse=True)
def reset_mock():
    global mock_service
    mock_service = MockCacheService()


# --- Tests ---


def test_default_key_builder():
    def my_func(a, b=2):
        pass

    key1 = _default_key_builder(my_func, 1, b=2)
    key2 = _default_key_builder(my_func, 1, b=2)
    key3 = _default_key_builder(my_func, 1, b=3)

    assert key1 == key2
    assert key1 != key3
    assert isinstance(key1, str) and len(key1) > 0


@pytest.mark.asyncio
async def test_cached_decorator_hit_miss():
    call_count = 0

    @cached(ttl=60, cache_service_provider=get_mock_service)
    async def expensive_func(arg):
        nonlocal call_count
        call_count += 1
        return f"result-{arg}"

    # First call (Miss)
    res1 = await expensive_func("a")
    assert res1 == "result-a"
    assert call_count == 1
    assert mock_service.get_called == 1
    assert mock_service.set_called == 1
    assert mock_service.store[list(mock_service.store.keys())[0]] == "result-a"

    # Second call (Hit)
    res2 = await expensive_func("a")
    assert res2 == "result-a"
    assert call_count == 1  # Function not called
    assert mock_service.get_called == 2

    # Different arg (Miss)
    res3 = await expensive_func("b")
    assert res3 == "result-b"
    assert call_count == 2


@pytest.mark.asyncio
async def test_cached_no_provider():
    # If provider returns None, should bypass cache transparently

    call_count = 0

    @cached(cache_service_provider=lambda: None)
    async def func():
        nonlocal call_count
        call_count += 1
        return "val"

    await func()
    await func()

    assert call_count == 2  # No caching


@pytest.mark.asyncio
async def test_cached_service_exception():
    # Service throws exception on get/set -> should proceed safely

    bad_service = MagicMock()
    bad_service.get = AsyncMock(side_effect=Exception("DB Down"))
    bad_service.set = AsyncMock(side_effect=Exception("DB Down"))

    call_count = 0

    @cached(cache_service_provider=lambda: bad_service)
    async def func():
        nonlocal call_count
        call_count += 1
        return "val"

    res = await func()
    assert res == "val"
    assert call_count == 1

    # Called again -> get fails -> func called -> set fails -> returns result
    res2 = await func()
    assert res2 == "val"
    assert call_count == 2


@pytest.mark.asyncio
async def test_invalidate_decorator():
    # Setup cache with value
    def key_builder(f, arg):
        return f"key:{arg}"
    await mock_service.set("key:123", "old_val")

    executed = False

    @cache_invalidate(key_builder=key_builder, cache_service_provider=get_mock_service)
    async def mod_func(arg):
        nonlocal executed
        executed = True
        return "done"

    res = await mod_func(123)

    assert res == "done"
    assert executed
    assert mock_service.delete_called == 1
    assert "key:123" not in mock_service.store


@pytest.mark.asyncio
async def test_invalidate_exception():
    bad_service = MagicMock()
    bad_service.delete = AsyncMock(side_effect=Exception("Fail"))

    @cache_invalidate(
        key_builder=lambda f: "k", cache_service_provider=lambda: bad_service
    )
    async def func():
        return "ok"

    # Should not raise
    assert await func() == "ok"
