from unittest.mock import MagicMock

from cqrs_ddd.contrib.dependency_injector import (
    Container,
    WiredThreadSafetyMiddleware,
    WiredEventStorePersistenceMiddleware,
    install,
)
from cqrs_ddd.middleware import middleware as middleware_registry


def test_container_initialization():
    container = Container()

    # Mock external dependencies
    container.config.from_dict(
        {
            "dramatiq": {"queues": ["default"], "threads": 1},
            "message_topic_router": {"routes": {}},
            "consumer": {"topics": [], "queue_name": "q"},
            "scan_packages": [],
        }
    )
    container.message_broker.override(MagicMock())
    container.uow_factory.override(MagicMock())
    container.outbox_storage.override(
        MagicMock()
    )  # Mock it to avoid init issues with real class

    # Verify core services are providing
    assert container.event_store()
    assert container.outbox_storage()
    assert container.outbox_service()
    assert container.mediator()
    assert container.saga_manager()


def test_wired_middleware_injection():
    # Verify injection works by inspecting the signature or defaults
    # Since we can't easily spin up a full wiring context without side effects,
    # we check if arguments have default injection markers

    from dependency_injector.wiring import Provide

    # Check default args
    import inspect

    sig = inspect.signature(WiredThreadSafetyMiddleware.__init__)
    # lock_strategy param is the 3rd one (self, entity_type, entity_id_attr, lock_strategy)
    param = sig.parameters["lock_strategy"]
    assert isinstance(param.default, Provide)


def test_install_hook():
    # Save original
    orig_ts = middleware_registry.classes.get("thread_safety")

    try:
        install()
        assert (
            middleware_registry.classes["thread_safety"] == WiredThreadSafetyMiddleware
        )
        assert (
            middleware_registry.classes["event_store"]
            == WiredEventStorePersistenceMiddleware
        )
    finally:
        # Restore
        if orig_ts:
            middleware_registry.classes["thread_safety"] = orig_ts
