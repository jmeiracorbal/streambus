from dataclasses import dataclass

import pytest

from streambus import EventPublisher, StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str


class MockTransport:
    def __init__(self):
        self.published: list[tuple[str, dict]] = []

    def publish(self, stream, data):
        self.published.append((stream, data))

    def read(self, stream, group, consumer, batch_size, block_ms):
        return []

    def ack(self, stream, group, msg_id):
        pass

    def ensure_group(self, stream, group):
        pass

    def reconnect(self):
        pass


@pytest.fixture
def transport():
    return MockTransport()


class TestEventPublisherValidation:
    def test_empty_stream_raises(self, transport):
        with pytest.raises(ConfigurationError, match="stream"):
            EventPublisher(stream="", transport=transport)

    def test_non_event_raises_on_publish(self, transport):
        publisher = EventPublisher(stream="test:events", transport=transport)
        with pytest.raises(EventValidationError, match="StreamBusEvent"):
            publisher.publish({"event_type": "bad"})  # type: ignore

    def test_missing_event_type_raises_on_publish(self, transport):
        publisher = EventPublisher(stream="test:events", transport=transport)
        with pytest.raises(EventValidationError, match="event_type"):
            publisher.publish(SampleEvent(event_type="", name="Acme"))


class TestEventPublisherIntegration:
    def test_publish_calls_transport(self, transport):
        publisher = EventPublisher(stream="test:events", transport=transport)
        event = SampleEvent(event_type="sample.created", name="Acme")
        publisher.publish(event)

        assert len(transport.published) == 1
        stream, data = transport.published[0]
        assert stream == "test:events"
        assert data["event_type"] == "sample.created"
        assert data["name"] == "Acme"
