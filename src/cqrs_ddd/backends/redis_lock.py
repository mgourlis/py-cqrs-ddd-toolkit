"""Redis distributed lock implementation."""
from typing import Any, List, Optional, Union
import uuid
import asyncio

try:
    from redis.asyncio import Redis
    from redis.exceptions import LockError
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False
    Redis = Any
    LockError = Exception


class RedisLockStrategy:
    """
    Redis-based distributed lock implementation with Fair (FCFS) semantics.
    
    Implements the LockStrategy protocol for ThreadSafetyMiddleware.
    Uses Redis ZSET (for queueing) and SET NX (for locking) managed via Lua scripts
    to ensure atomic, first-come-first-served acquisition.
    
    Features:
    - Fair Locking: Requests are served in order of arrival (FCFS).
    - Atomic Queue Management: Lua scripts prevent race conditions.
    - Deadlock Prevention: Sorted resource locking and zombie cleanup.
    - Automatic Expiration: Locks expire automatically if not released.
    """
    
    def __init__(
        self,
        redis: 'Redis',
        prefix: str = "lock",
        default_timeout: int = 30,
        retry_interval: float = 0.1,
        max_retries: int = 100
    ):
        if not HAS_REDIS:
            raise ImportError(
                "redis is required. Install with: pip install py-cqrs-ddd-toolkit[redis]"
            )
        
        self.redis = redis
        self.prefix = prefix
        self.default_timeout = default_timeout
        self.retry_interval = retry_interval
        # max_retries essentially limits the total wait time in the queue
        self.max_retries = max_retries
    
    def _lock_key(self, entity_type: str, entity_id: Any) -> str:
        """Generate lock key (holds the owner token)."""
        return f"{self.prefix}:{entity_type}:{entity_id}"

    def _queue_key(self, entity_type: str, entity_id: Any) -> str:
        """Generate queue key (ZSET of waiters)."""
        return f"{self.prefix}:queue:{entity_type}:{entity_id}"
    
    async def acquire(
        self,
        entity_type: str,
        entity_id_or_ids: Union[Any, List[Any]],
        timeout: int = None
    ) -> str:
        """
        Acquire lock(s) for entity/entities in a Fair (FCFS) manner.
        """
        timeout = timeout or self.default_timeout
        token = str(uuid.uuid4())
        
        # Normalize and sort IDs to prevent deadlocks when locking multiple resources
        ids = entity_id_or_ids if isinstance(entity_id_or_ids, list) else [entity_id_or_ids]
        sorted_ids = sorted(str(id) for id in ids)
        
        acquired_keys = []
        try:
            for entity_id in sorted_ids:
                success = await self._acquire_single_fair(entity_type, entity_id, token, timeout)
                if not success:
                    # Rolling back: Release all locks acquired so far
                    # Note: We must also remove ourselves from the queue if we gave up
                    await self._release_all(entity_type, sorted_ids, token)
                    raise TimeoutError(
                        f"Could not acquire fair lock for {entity_type}:{entity_id} within timeout"
                    )
                
                acquired_keys.append(entity_id)
            
            return token
        
        except Exception:
            # Cleanup on error
            await self._release_all(entity_type, sorted_ids, token)
            raise
    
    async def _acquire_single_fair(
        self,
        entity_type: str,
        entity_id: Any,
        token: str,
        timeout: int
    ) -> bool:
        """
        Acquire a single lock using Fair algorithm.
        
        Algorithm:
        1. Add self to ZSET (Queue) with score=NOW.
        2. Loop until timeout:
           a. Check Rank in Queue.
           b. If Rank 0 (Head): Try SET NX.
           c. If SET NX succeeds: Return True (Acquired).
           d. If not Head or SET NX fails: Sleep and retry.
        3. If timeout: Remove self from Queue.
        """
        lock_key = self._lock_key(entity_type, entity_id)
        queue_key = self._queue_key(entity_type, entity_id)
        timeout_ms = timeout * 1000
        
        # Script to attempt acquisition if we are at the head of the queue
        # Keys: {queue_key}, {lock_key}
        # Args: {token}, {timeout_ms}, {current_timestamp}
        # Returns: 
        #   0: Still in queue (not head, or lock held by other)
        #   1: Acquired
        fair_acquire_script = """
        local queue_key = KEYS[1]
        local lock_key = KEYS[2]
        local token = ARGV[1]
        local timeout_ms = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        
        -- 1. Ensure we are in the queue (idempotent ZADD)
        -- We use 'NX' to keep original arrival time if already there
        redis.call('ZADD', queue_key, 'NX', now, token)
        
        -- 2. Clean up zombies (optional lazy cleanup)
        -- If head is older than 2x timeout, remove it (safety valve)
        local head = redis.call('ZRANGE', queue_key, 0, 0, 'WITHSCORES')
        if #head > 0 then
            local head_score = tonumber(head[2])
            if (now - head_score) > (timeout_ms / 1000 * 2) then
                redis.call('ZREM', queue_key, head[1])
            end
        end

        -- 3. Check Rank
        local rank = redis.call('ZRANK', queue_key, token)
        
        -- If we are at the front (rank 0)
        if rank == 0 then
            -- Try to acquire the lock
            local acquired = redis.call('SET', lock_key, token, 'NX', 'PX', timeout_ms)
            if acquired then
                -- Success! remove us from queue.
                redis.call('ZREM', queue_key, token)
                return 1
            else
                -- Lock is held by someone else or previous owner hasn't released yet.
                return 0
            end
        end
        return 0
        """
        
        import time
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            now_ts = time.time()
            if (now_ts - start_time) > timeout:
                break

            result = await self.redis.eval(
                fair_acquire_script,
                2,
                queue_key,
                lock_key,
                token,
                timeout_ms,
                now_ts
            )
            
            if result == 1:
                return True
            
            await asyncio.sleep(self.retry_interval)
            
        # Cleanup if failed
        await self.redis.zrem(queue_key, token)
        return False

    async def release(
        self,
        entity_type: str,
        entity_id_or_ids: Union[Any, List[Any]],
        token: str
    ) -> None:
        """Release lock(s)."""
        ids = entity_id_or_ids if isinstance(entity_id_or_ids, list) else [entity_id_or_ids]
        await self._release_all(entity_type, sorted(str(id) for id in ids), token)
        
    async def _release_all(self, entity_type: str, ids: List[str], token: str) -> None:
        """Atomic release of multiple locks."""
        if not ids:
             return

        # Prepare interleaved keys: lock1, queue1, lock2, queue2...
        keys = []
        for entity_id in ids:
            keys.append(self._lock_key(entity_type, entity_id))
            keys.append(self._queue_key(entity_type, entity_id))
            
        release_all_script = """
        local token = ARGV[1]
        for i = 1, #KEYS, 2 do
            local lock_key = KEYS[i]
            local queue_key = KEYS[i+1]
            
            if redis.call("GET", lock_key) == token then
                redis.call("DEL", lock_key)
            end
            redis.call("ZREM", queue_key, token)
        end
        return 1
        """
        
        await self.redis.eval(release_all_script, len(keys), *keys, token)

    async def extend(
        self,
        entity_type: str,
        entity_id: Any,
        token: str,
        additional_time: int = None
    ) -> bool:
        """Extend lock timeout."""
        additional_time = additional_time or self.default_timeout
        key = self._lock_key(entity_type, entity_id)
        
        extend_script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("PEXPIRE", KEYS[1], ARGV[2])
        end
        return 0
        """
        result = await self.redis.eval(
            extend_script, 1, key, token, additional_time * 1000
        )
        return result == 1

    async def is_locked(self, entity_type: str, entity_id: Any) -> bool:
        """Check if locked."""
        return await self.redis.exists(self._lock_key(entity_type, entity_id)) > 0
    
    async def get_lock_owner(self, entity_type: str, entity_id: Any) -> Optional[str]:
        token = await self.redis.get(self._lock_key(entity_type, entity_id))
        return token.decode() if token else None


class InMemoryLockStrategy:
    """
    In-memory lock implementation for testing.
    
    NOT for production use - only works within a single process.
    """
    
    def __init__(self, timeout: int = 30, retry_interval: float = 0.1, max_retries: int = 100):
        self._locks: dict = {}
        self.timeout = timeout
        self.retry_interval = retry_interval
        self.max_retries = max_retries
    
    async def acquire(
        self,
        entity_type: str,
        entity_id_or_ids: Union[Any, List[Any]],
        timeout: int = None
    ) -> str:
        """Acquire locks."""
        token = str(uuid.uuid4())
        ids = entity_id_or_ids if isinstance(entity_id_or_ids, list) else [entity_id_or_ids]
        sorted_ids = sorted(str(id) for id in ids)
        
        acquired = []
        for entity_id in sorted_ids:
            key = f"{entity_type}:{entity_id}"
            
            for _ in range(self.max_retries):
                if key not in self._locks:
                    self._locks[key] = token
                    acquired.append(key)
                    break
                await asyncio.sleep(self.retry_interval)
            else:
                # Release acquired and fail
                for k in acquired:
                    if self._locks.get(k) == token:
                        del self._locks[k]
                raise TimeoutError(f"Could not acquire lock for {key}")
        
        return token
    
    async def release(
        self,
        entity_type: str,
        entity_id_or_ids: Union[Any, List[Any]],
        token: str
    ) -> None:
        """Release locks."""
        ids = entity_id_or_ids if isinstance(entity_id_or_ids, list) else [entity_id_or_ids]
        
        for entity_id in ids:
            key = f"{entity_type}:{entity_id}"
            if self._locks.get(key) == token:
                del self._locks[key]
    
    def clear(self) -> None:
        """Clear all locks (for testing)."""
        self._locks.clear()
