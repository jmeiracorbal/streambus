from dataclasses import dataclass

import pytest

from streambus import AsyncEventPublisher, StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str


class MockAsyncTransport:
    def __init__(self):
        self.published: list[tuple[str, dict]] = []

    async def publish(self, stream, data):
        self.published.append((stream, data))

    async def read(self, stream, group, consumer, batch_size, block_ms):
        return []

    async def ack(self, stream, group, msg_id):
        pass

    async def ensure_group(self, stream, group):
        pass

    async def reconnect(self):
        pass


@pytest.fixture
def transport():
    return MockAsyncTransport()


class TestAsyncEventPublisherValidation:
    def test_empty_stream_raises(self, transport):
        with pytest.raises(ConfigurationError, match="stream"):
            AsyncEventPublisher(stream="", transport=transport)

    @pytest.mark.asyncio
    async def test_non_event_raises_on_publish(self, transport):
        publisher = AsyncEventPublisher(stream="test:events", transport=transport)
        with pytest.raises(EventValidationError, match="StreamBusEvent"):
            await publisher.publish({"event_type": "bad"})  # type: ignore

    @pytest.mark.asyncio
    async def test_missing_event_type_raises_on_publish(self, transport):
        publisher = AsyncEventPublisher(stream="test:events", transport=transport)
        with pytest.raises(EventValidationError, match="event_type"):
            await publisher.publish(SampleEvent(event_type="", name="Acme"))


class TestAsyncEventPublisherIntegration:
    @pytest.mark.asyncio
    async def test_publish_calls_transport(self, transport):
        publisher = AsyncEventPublisher(stream="test:events", transport=transport)
        event = SampleEvent(event_type="sample.created", name="Acme")
        await publisher.publish(event)

        assert len(transport.published) == 1
        stream, data = transport.published[0]
        assert stream == "test:events"
        assert data["event_type"] == "sample.created"
        assert data["name"] == "Acme"
