from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from streambus import EventPublisher, RedisConfig, StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str


@pytest.fixture
def config():
    return RedisConfig(url="redis://localhost:6379")


class TestEventPublisherValidation:
    def test_empty_stream_raises(self, config):
        with pytest.raises(ConfigurationError, match="stream"):
            EventPublisher(stream="", config=config)

    def test_invalid_config_raises(self):
        with pytest.raises(ConfigurationError, match="config"):
            EventPublisher(stream="s", config="bad")

    def test_non_event_raises_on_publish(self, config):
        publisher = EventPublisher(stream="test:events", config=config)
        with pytest.raises(EventValidationError, match="StreamBusEvent"):
            publisher.publish({"event_type": "bad"})  # type: ignore

    def test_missing_event_type_raises_on_publish(self, config):
        publisher = EventPublisher(stream="test:events", config=config)
        with pytest.raises(EventValidationError, match="event_type"):
            publisher.publish(SampleEvent(event_type="", name="Acme"))


class TestEventPublisherIntegration:
    def test_publish_calls_xadd(self, config):
        mock_redis = MagicMock()
        publisher = EventPublisher(stream="test:events", config=config)
        publisher._redis = mock_redis

        event = SampleEvent(event_type="sample.created", name="Acme")
        publisher.publish(event)

        mock_redis.xadd.assert_called_once_with("test:events", event.to_dict())
