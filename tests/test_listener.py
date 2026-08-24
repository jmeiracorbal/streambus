from dataclasses import dataclass

import pytest

from streambus import EventListener, StreamBusEvent
from streambus.exceptions import ConfigurationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str


class MockTransport:
    def __init__(self, messages: list[tuple[str, dict]] | None = None):
        self._messages = list(messages or [])
        self.acked: list[str] = []
        self.groups: list[str] = []

    def publish(self, stream, data):
        self._messages.append(("mock-id", data))

    def read(self, stream, group, consumer, batch_size, block_ms):
        batch, self._messages = self._messages[:batch_size], self._messages[batch_size:]
        return batch

    def ack(self, stream, group, msg_id):
        self.acked.append(msg_id)

    def ensure_group(self, stream, group):
        self.groups.append(f"{stream}:{group}")

    def reconnect(self):
        pass


@pytest.fixture
def transport():
    return MockTransport()


class TestEventListenerValidation:
    def test_empty_stream_raises(self, transport):
        with pytest.raises(ConfigurationError, match="stream"):
            EventListener(stream="", group="g", handler=lambda e: None, event_class=SampleEvent, transport=transport)

    def test_empty_group_raises(self, transport):
        with pytest.raises(ConfigurationError, match="group"):
            EventListener(stream="s", group="", handler=lambda e: None, event_class=SampleEvent, transport=transport)

    def test_handler_not_callable_raises(self, transport):
        with pytest.raises(ConfigurationError, match="handler"):
            EventListener(stream="s", group="g", handler="not_callable", event_class=SampleEvent, transport=transport)

    def test_event_class_not_subclass_raises(self, transport):
        with pytest.raises(ConfigurationError, match="StreamBusEvent"):
            EventListener(stream="s", group="g", handler=lambda e: None, event_class=dict, transport=transport)


class TestEventListenerIntegration:
    def test_handler_receives_typed_event(self):
        received: list[SampleEvent] = []
        messages = [("msg-1", {"event_type": "sample.created", "name": "Acme", "occurred_at": "2026-01-01T00:00:00+00:00"})]
        transport = MockTransport(messages=messages)

        listener = EventListener(
            stream="test:events",
            group="test",
            handler=received.append,
            event_class=SampleEvent,
            transport=transport,
        )

        batch = transport.read("test:events", "test", "test-1", 10, 0)
        for msg_id, data in batch:
            event = listener.event_class.from_dict(data)
            listener.handler(event)
            transport.ack("test:events", "test", msg_id)

        assert len(received) == 1
        assert isinstance(received[0], SampleEvent)
        assert received[0].name == "Acme"
        assert transport.acked == ["msg-1"]

    def test_unknown_stream_fields_are_ignored(self):
        received: list[SampleEvent] = []
        messages = [("msg-1", {"event_type": "sample.created", "name": "Beta", "occurred_at": "2026-01-01T00:00:00+00:00", "extra_field": "ignored"})]
        transport = MockTransport(messages=messages)

        listener = EventListener(
            stream="test:events",
            group="test",
            handler=received.append,
            event_class=SampleEvent,
            transport=transport,
        )

        batch = transport.read("test:events", "test", "test-1", 10, 0)
        for msg_id, data in batch:
            event = listener.event_class.from_dict(data)
            listener.handler(event)

        assert received[0].name == "Beta"
        assert not hasattr(received[0], "extra_field")
