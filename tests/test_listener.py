from dataclasses import dataclass
from unittest.mock import patch

import fakeredis
import pytest

from streambus import EventListener, RedisConfig, StreamBusEvent
from streambus.exceptions import ConfigurationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    name: str


@pytest.fixture
def config():
    return RedisConfig(url="redis://localhost:6379")


@pytest.fixture
def fake_redis():
    return fakeredis.FakeRedis(decode_responses=True)


class TestEventListenerValidation:
    def test_empty_stream_raises(self, config):
        with pytest.raises(ConfigurationError, match="stream"):
            EventListener(stream="", group="g", handler=lambda e: None, event_class=SampleEvent, config=config)

    def test_empty_group_raises(self, config):
        with pytest.raises(ConfigurationError, match="group"):
            EventListener(stream="s", group="", handler=lambda e: None, event_class=SampleEvent, config=config)

    def test_handler_not_callable_raises(self, config):
        with pytest.raises(ConfigurationError, match="handler"):
            EventListener(stream="s", group="g", handler="not_callable", event_class=SampleEvent, config=config)

    def test_event_class_not_subclass_raises(self, config):
        with pytest.raises(ConfigurationError, match="StreamBusEvent"):
            EventListener(stream="s", group="g", handler=lambda e: None, event_class=dict, config=config)

    def test_invalid_config_type_raises(self):
        with pytest.raises(ConfigurationError, match="config"):
            EventListener(stream="s", group="g", handler=lambda e: None, event_class=SampleEvent, config="bad")

    def test_empty_config_url_raises(self):
        with pytest.raises(ConfigurationError, match="config.url"):
            EventListener(stream="s", group="g", handler=lambda e: None, event_class=SampleEvent, config=RedisConfig(url=""))


class TestEventListenerIntegration:
    def test_handler_receives_typed_event(self, config, fake_redis):
        received: list[SampleEvent] = []

        listener = EventListener(
            stream="test:events",
            group="test",
            handler=received.append,
            event_class=SampleEvent,
            config=config,
        )

        fake_redis.xadd("test:events", {"event_type": "sample.created", "name": "Acme", "occurred_at": "2026-01-01T00:00:00+00:00"})

        with patch.object(listener, "_connect", return_value=fake_redis):
            listener._ensure_group(fake_redis)
            results = fake_redis.xreadgroup("test", "test-1", {"test:events": ">"}, count=1, block=0)
            for _stream, messages in (results or []):
                for msg_id, data in messages:
                    event = listener.event_class.from_dict(data)
                    listener.handler(event)
                    fake_redis.xack("test:events", "test", msg_id)

        assert len(received) == 1
        assert isinstance(received[0], SampleEvent)
        assert received[0].name == "Acme"

    def test_unknown_stream_fields_are_ignored(self, config, fake_redis):
        received: list[SampleEvent] = []

        listener = EventListener(
            stream="test:events",
            group="test",
            handler=received.append,
            event_class=SampleEvent,
            config=config,
        )

        fake_redis.xadd("test:events", {"event_type": "sample.created", "name": "Beta", "occurred_at": "2026-01-01T00:00:00+00:00", "extra_field": "ignored"})

        with patch.object(listener, "_connect", return_value=fake_redis):
            listener._ensure_group(fake_redis)
            results = fake_redis.xreadgroup("test", "test-1", {"test:events": ">"}, count=1, block=0)
            for _stream, messages in (results or []):
                for msg_id, data in messages:
                    event = listener.event_class.from_dict(data)
                    listener.handler(event)

        assert received[0].name == "Beta"
        assert not hasattr(received[0], "extra_field")
