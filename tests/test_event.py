from dataclasses import dataclass

import pytest

from streambus import StreamBusEvent


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str
    value: str


class TestStreamBusEvent:
    def test_to_dict_returns_all_fields_as_strings(self):
        event = SampleEvent(event_type="sample.created", name="Acme", value="42")
        d = event.to_dict()
        assert d["event_type"] == "sample.created"
        assert d["name"] == "Acme"
        assert d["value"] == "42"
        assert all(isinstance(v, str) for v in d.values())

    def test_to_dict_includes_occurred_at(self):
        event = SampleEvent(event_type="sample.created", name="Acme", value="1")
        d = event.to_dict()
        assert "occurred_at" in d
        assert d["occurred_at"] != ""

    def test_from_dict_maps_known_fields(self):
        data = {"event_type": "sample.created", "name": "Acme", "value": "42", "occurred_at": "2026-01-01T00:00:00+00:00"}
        event = SampleEvent.from_dict(data)
        assert isinstance(event, SampleEvent)
        assert event.event_type == "sample.created"
        assert event.name == "Acme"
        assert event.value == "42"

    def test_from_dict_ignores_unknown_fields(self):
        data = {"event_type": "sample.created", "name": "Acme", "value": "1", "unknown": "ignored"}
        event = SampleEvent.from_dict(data)
        assert not hasattr(event, "unknown")

    def test_from_dict_missing_required_field_raises(self):
        data = {"event_type": "sample.created", "name": "Acme"}  # value missing
        with pytest.raises(TypeError):
            SampleEvent.from_dict(data)
