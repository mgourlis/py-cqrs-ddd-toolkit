import pytest
from dataclasses import dataclass
from unittest.mock import patch

from cqrs_ddd.domain_event import (
    DomainEventBase,
    generate_event_id,
    generate_correlation_id,
    enrich_event_metadata,
)

# --- Helpers ---


@dataclass
class MutableEvent:
    data: str
    correlation_id: str = None
    causation_id: str = None


class FrozenEvent:
    def __init__(self, data, **kwargs):
        # Bypass __setattr__ to initialize
        self.__dict__["data"] = data
        self.__dict__["_kwargs"] = kwargs

    def __getattr__(self, item):
        if item in self._kwargs:
            return self._kwargs[item]
        # Simulate optional metadata fields existing but being None
        if item in ("correlation_id", "causation_id"):
            return None
        raise AttributeError(f"'FrozenEvent' object has no attribute '{item}'")

    def __setattr__(self, key, value):
        raise AttributeError("Immutable")

    def copy(self, update=None):
        new_kwargs = self._kwargs.copy()
        if update:
            new_kwargs.update(update)
        return FrozenEvent(self.data, **new_kwargs)


class PydanticEvent:
    def __init__(self, data, **kwargs):
        self.__dict__["data"] = data
        self.__dict__["_kwargs"] = kwargs

    def __getattr__(self, item):
        if item in self._kwargs:
            return self._kwargs[item]
        if item in ("correlation_id", "causation_id"):
            return None
        raise AttributeError(f"'PydanticEvent' object has no attribute '{item}'")

    def __setattr__(self, key, value):
        raise AttributeError("Immutable")

    def model_copy(self, update=None):
        new_kwargs = self._kwargs.copy()
        if update:
            new_kwargs.update(update)
        return PydanticEvent(self.data, **new_kwargs)


# --- Tests ---


def test_id_generation():
    id1 = generate_event_id()
    id2 = generate_event_id()
    assert id1 != id2
    assert isinstance(id1, str)

    cid1 = generate_correlation_id()
    cid2 = generate_correlation_id()
    assert cid1 != cid2


def test_enrich_mutable():
    evt = MutableEvent(data="test")
    enriched = enrich_event_metadata(evt, correlation_id="corr-1")

    assert enriched is evt  # Same instance
    assert evt.correlation_id == "corr-1"
    assert evt.causation_id is None  # Not set


def test_enrich_frozen_copy():
    evt = FrozenEvent(data="test")
    enriched = enrich_event_metadata(evt, correlation_id="corr-1")

    assert enriched is not evt  # New instance
    assert enriched.correlation_id == "corr-1"
    assert evt.correlation_id is None  # Original untouched


def test_enrich_pydantic_model_copy():
    evt = PydanticEvent(data="test")
    enriched = enrich_event_metadata(evt, correlation_id="corr-2")

    assert enriched is not evt
    assert enriched.correlation_id == "corr-2"


def test_enrich_preserves_existing():
    evt = MutableEvent(data="test", correlation_id="existing-corr")
    enriched = enrich_event_metadata(evt, correlation_id="new-corr")

    assert enriched.correlation_id == "existing-corr"


@dataclass
class ConcreteEvent(DomainEventBase):
    field1: str = ""

    @property
    def aggregate_id(self):
        return "agg-1"

    @property
    def aggregate_type(self):
        return "TestAggregate"


def test_base_class_serialization():
    evt = ConcreteEvent(field1="val")

    # Test default base serialization
    data = evt.to_dict()

    # Base implementation ONLY includes base fields
    assert "field1" not in data
    assert data["event_id"] == evt.event_id
    assert data["aggregate_id"] == "agg-1"
    assert data["aggregate_type"] == "TestAggregate"
    assert data["event_type"] == "ConcreteEvent"


def test_base_class_abstract_methods():
    @dataclass
    class IncompleteEvent(DomainEventBase):
        pass

    evt = IncompleteEvent()

    # aggregate_id is abstract-ish (raises NotImplementedError)
    with pytest.raises(NotImplementedError):
        _ = evt.aggregate_id

    # from_dict is abstract
    with pytest.raises(NotImplementedError):
        IncompleteEvent.from_dict({})


def test_auto_registration():
    # We need to check if EventTypeRegistry.register_class was called.
    # Since it happens at class definition time, it's hard to patch unless we define class inside test function with patch active.

    with patch("cqrs_ddd.event_registry.EventTypeRegistry.register_class") as mock_reg:

        @dataclass
        class NewRegisteredEvent(DomainEventBase):
            @property
            def aggregate_id(self):
                return "1"

        mock_reg.assert_called_with(NewRegisteredEvent)
