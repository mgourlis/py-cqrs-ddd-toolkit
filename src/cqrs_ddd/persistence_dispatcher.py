"""Unified Persistence Dispatcher - auto-resolves command and query handlers."""

from abc import ABC, abstractmethod
from typing import (
    Type,
    Dict,
    Callable,
    Union,
    List,
    Any,
    Optional,
    TypeVar,
    Generic,
    get_args,
    get_origin,
    TYPE_CHECKING,
)
import logging
from dataclasses import dataclass

if TYPE_CHECKING:
    from .protocols import CacheService

logger = logging.getLogger("cqrs_ddd")

T_Mod = TypeVar("T_Mod")  # Modification type for commands
T_Entity = TypeVar("T_Entity")  # Domain Entity type
T_Result = TypeVar("T_Result")  # Result type for queries


@dataclass
class HandlerEntry:
    """Registry entry with priority."""

    handler_cls: Type
    priority: int = 0  # Higher = runs first


# === REGISTRIES (support multiple handlers) ===
_modification_handlers: Dict[Type, List[HandlerEntry]] = {}
_retrieval_handlers: Dict[Type, List[HandlerEntry]] = {}
_query_handlers: Dict[Type, List[HandlerEntry]] = {}


def _register_handler(registry: Dict, key: Type, handler_cls: Type, priority: int):
    """Register a handler with priority support."""
    if key not in registry:
        registry[key] = []

    # Check if already registered (avoid duplicates)
    for entry in registry[key]:
        if entry.handler_cls is handler_cls:
            return

    registry[key].append(HandlerEntry(handler_cls=handler_cls, priority=priority))
    # Sort by priority (descending - higher priority first)
    registry[key].sort(key=lambda e: e.priority, reverse=True)
    logger.debug(
        f"Registered {handler_cls.__name__} for {key.__name__} (priority={priority})"
    )


def _get_handlers(registry: Dict, key: Type) -> List[Type]:
    """Get handlers sorted by priority."""
    entries = registry.get(key, [])
    return [e.handler_cls for e in entries]


def _extract_generic_args(cls: Type, base_generic: Type) -> List[Type]:
    """Extract type arguments from a generic base class, supporting Unions."""
    for base in getattr(cls, "__orig_bases__", []):
        origin = get_origin(base)
        # Check if origin is the base generic OR a subclass of it
        if origin is base_generic or (
            isinstance(origin, type) and issubclass(origin, base_generic)
        ):
            args = get_args(base)
            if not args or isinstance(args[0], TypeVar):
                return []

            # Check if it's a Union
            arg = args[0]
            origin_arg = get_origin(arg)
            if origin_arg is Union:
                return list(get_args(arg))

            return [arg]
        # We don't log 'skipping' anymore to avoid noise from Generic/ABC/etc.
    return []


# === INTROSPECTION ===


def list_modification_handlers() -> Dict[str, List[str]]:
    return {
        k.__name__: [e.handler_cls.__name__ for e in v]
        for k, v in _modification_handlers.items()
    }


def list_retrieval_handlers() -> Dict[str, List[str]]:
    return {
        k.__name__: [e.handler_cls.__name__ for e in v]
        for k, v in _retrieval_handlers.items()
    }


def list_query_handlers() -> Dict[str, List[str]]:
    return {
        k.__name__: [e.handler_cls.__name__ for e in v]
        for k, v in _query_handlers.items()
    }


# === BASE CLASSES WITH AUTO-REGISTRATION ===


class OperationPersistence(ABC, Generic[T_Mod]):
    """
    Base class for command-side persistence (Writes).

    Supports multiple handlers per modification type.
    Set class attribute `priority` for ordering (higher = first).
    """

    priority: int = 0
    use_cache: bool = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        mod_types = _extract_generic_args(cls, OperationPersistence)
        for mod_type in mod_types:
            _register_handler(
                _modification_handlers, mod_type, cls, getattr(cls, "priority", 0)
            )

        # Diagnostic: If no types found and not an intermediate generic class
        if not mod_types and not any(
            isinstance(a, TypeVar)
            for b in getattr(cls, "__orig_bases__", [])
            for a in get_args(b)
        ):
            logger.warning(
                f"Class {cls.__name__} inherits from OperationPersistence but specifies no modification types."
            )

    @abstractmethod
    async def persist(
        self, modification: T_Mod, unit_of_work: Any
    ) -> Union[Any, List[Any]]:
        """
        Persist a modification.

        Args:
            modification: The modification DTO.
            unit_of_work: The active Unit of Work / DB session.

        Returns:
            The ID of the created/updated entity, or list of IDs.
        """
        raise NotImplementedError


class RetrievalPersistence(ABC, Generic[T_Entity]):
    """
    Base class for domain entity retrieval (Command-Side Reads).

    Cache Key: "EntityName:ID"
    """

    entity_name: Optional[str] = None
    cache_ttl: int = 300
    priority: int = 0
    use_cache: bool = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        entity_types = _extract_generic_args(cls, RetrievalPersistence)
        for entity_type in entity_types:
            _register_handler(
                _retrieval_handlers, entity_type, cls, getattr(cls, "priority", 0)
            )

        # Diagnostic
        if not entity_types and not any(
            isinstance(a, TypeVar)
            for b in getattr(cls, "__orig_bases__", [])
            for a in get_args(b)
        ):
            logger.warning(
                f"Class {cls.__name__} inherits from RetrievalPersistence but specifies no entity types."
            )

    @abstractmethod
    async def retrieve(
        self, entity_ids: List[Any], unit_of_work: Any
    ) -> List[T_Entity]:
        """Retrieve domain entities by IDs."""
        raise NotImplementedError


class QueryPersistence(ABC, Generic[T_Result]):
    """
    Base class for query-side persistence (Read Models).

    Cache Key: "EntityName:query:ID"
    """

    entity_name: Optional[str] = None
    cache_ttl: int = 300
    priority: int = 0
    use_cache: bool = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        result_types = _extract_generic_args(cls, QueryPersistence)
        for result_type in result_types:
            _register_handler(
                _query_handlers, result_type, cls, getattr(cls, "priority", 0)
            )

    @abstractmethod
    async def fetch(self, entity_ids: List[Any], unit_of_work: Any) -> List[T_Result]:
        """Fetch result DTOs by IDs."""
        raise NotImplementedError


# === UNIFIED DISPATCHER ===


class PersistenceDispatcher:
    """
    Unified dispatcher for command (modifications/retrieval) and query persistence.

    Supports multiple handlers per modification with priority ordering.
    """

    def __init__(
        self,
        uow_factory: Callable,
        cache_service: Optional["CacheService"] = None,
        handler_factory: Optional[Callable[[Type], Any]] = None,
    ):
        self._uow_factory = uow_factory
        self._cache_service = cache_service
        self._handler_factory = handler_factory or (lambda cls: cls())

    def transaction(self) -> Any:
        """
        Start a new Unit of Work (transaction).

        Usage:
            async with dispatcher.transaction() as uow:
                ...
        """
        return self._uow_factory()

    # === COMMAND SIDE (Writes) ===

    async def apply(
        self, modification: Any, unit_of_work: Optional[Any] = None
    ) -> Union[Any, List[Any]]:
        """
        Apply a modification (Write).

        Orchestrates the persistence flow:
        1. Starts a Unit of Work (if not provided).
        2. Resolves registered handlers for the modification type.
        3. Executes handlers in priority order.
        4. Invalidates cache based on result (if caching enabled).

        Args:
            modification: The command-side modification DTO.
            unit_of_work: Optional existing UOW.

        Returns:
            Result from the highest priority handler (usually Entity ID).
        """
        mod_type = type(modification)
        handler_classes = _get_handlers(_modification_handlers, mod_type)

        if not handler_classes:
            raise ValueError(f"No handler registered for {mod_type.__name__}")

        result = None

        async def execute(uow):
            nonlocal result
            for i, handler_cls in enumerate(handler_classes):
                handler = self._handler_factory(handler_cls)
                h_result = await handler.persist(modification, uow)
                if i == 0:
                    result = h_result
            return result

        if unit_of_work:
            await execute(unit_of_work)
        else:
            async with self._uow_factory() as uow:
                await execute(uow)

        # Cache Invalidation: Skip if any handler has use_cache=False
        should_cache = all(
            getattr(self._handler_factory(h), "use_cache", True)
            for h in handler_classes
        )

        if self._cache_service and result and should_cache:
            await self._invalidate_cache(modification, result)

        return result

    # === COMMAND SIDE (Reads) ===

    async def fetch_domain(
        self,
        entity_type: Type[T_Entity],
        entity_ids: List[Any],
        unit_of_work: Optional[Any] = None,
    ) -> List[T_Entity]:
        """
        Fetch Domain Entities (Aggregates).

        Uses first registered handler (highest priority).
        """
        handler_classes = _get_handlers(_retrieval_handlers, entity_type)
        if not handler_classes:
            raise ValueError(
                f"No retrieval handler registered for {entity_type.__name__}"
            )

        handler = self._handler_factory(handler_classes[0])

        # Read-Through Cache (Domain)
        if (
            self._cache_service
            and handler.entity_name
            and getattr(handler, "use_cache", True)
        ):
            return await self._execute_read_through(
                handler, entity_ids, unit_of_work, cache_key_suffix=""
            )

        if unit_of_work:
            return await handler.retrieve(entity_ids, unit_of_work)
        else:
            async with self._uow_factory() as uow:
                return await handler.retrieve(entity_ids, uow)

    async def fetch_domain_one(
        self,
        entity_type: Type[T_Entity],
        entity_id: Any,
        unit_of_work: Optional[Any] = None,
    ) -> Optional[T_Entity]:
        """Fetch single domain entity."""
        results = await self.fetch_domain(entity_type, [entity_id], unit_of_work)
        return results[0] if results else None

    # === QUERY SIDE (Reads) ===

    async def fetch(
        self,
        result_type: Type[T_Result],
        entity_ids: List[Any],
        unit_of_work: Optional[Any] = None,
    ) -> List[T_Result]:
        """
        Fetch Read Models (DTOs).

        Uses first registered handler (highest priority).
        """
        handler_classes = _get_handlers(_query_handlers, result_type)
        if not handler_classes:
            raise ValueError(f"No query handler registered for {result_type.__name__}")

        handler = self._handler_factory(handler_classes[0])

        # Read-Through Cache (Query)
        if (
            self._cache_service
            and handler.entity_name
            and getattr(handler, "use_cache", True)
        ):
            return await self._execute_read_through(
                handler, entity_ids, unit_of_work, cache_key_suffix=":query"
            )

        if unit_of_work:
            return await handler.fetch(entity_ids, unit_of_work)
        else:
            async with self._uow_factory() as uow:
                return await handler.fetch(entity_ids, uow)

    async def _execute_read_through(
        self,
        handler: Any,
        entity_ids: List[Any],
        unit_of_work: Optional[Any],
        cache_key_suffix: str,
    ) -> List[Any]:
        """Execute Read-Through caching strategy."""
        entity_name = handler.entity_name

        keys = [f"{entity_name}{cache_key_suffix}:{eid}" for eid in entity_ids]

        try:
            cached_values = await self._cache_service.get_batch(keys)
        except Exception:
            cached_values = [None] * len(entity_ids)

        results = []
        missing_ids = []

        for eid, val in zip(entity_ids, cached_values):
            if val is not None:
                results.append(val)
            else:
                missing_ids.append(eid)

        if not missing_ids:
            return results

        method = getattr(handler, "retrieve", getattr(handler, "fetch", None))

        if unit_of_work:
            fresh_results = await method(missing_ids, unit_of_work)
        else:
            async with self._uow_factory() as uow:
                fresh_results = await method(missing_ids, uow)

        if fresh_results:
            to_cache = []
            for item in fresh_results:
                item_id = getattr(item, "id", None)
                if item_id:
                    key = f"{entity_name}{cache_key_suffix}:{item_id}"
                    to_cache.append({"cache_key": key, "value": item})

            if to_cache:
                try:
                    await self._cache_service.set_batch(to_cache, ttl=handler.cache_ttl)
                except Exception:
                    pass

            results.extend(fresh_results)

        return results

    async def fetch_one(
        self,
        result_type: Type[T_Result],
        entity_id: Any,
        unit_of_work: Optional[Any] = None,
    ) -> Optional[T_Result]:
        """Convenience method to fetch a single entity."""
        results = await self.fetch(result_type, [entity_id], unit_of_work)
        return results[0] if results else None

    # === CACHE INVALIDATION ===

    async def _invalidate_cache(self, modification: Any, result: Union[Any, List[Any]]):
        """Invalidate both domain and query caches."""
        entity_name = type(modification.entity).__name__

        ids = result if isinstance(result, list) else [result]
        keys = []

        for id in ids:
            if id is not None:
                keys.append(f"{entity_name}:{id}")
                keys.append(f"{entity_name}:query:{id}")

        if keys:
            try:
                await self._cache_service.delete_batch(keys)
                logger.debug(f"Invalidated cache: {keys}")
            except Exception as e:
                logger.warning(f"Cache invalidation failed: {e}")
