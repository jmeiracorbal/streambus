import logging

import redis as redis_lib

from streambus.config import RedisConfig
from streambus.event import StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, stream: str, config: RedisConfig):
        if not stream:
            raise ConfigurationError("stream is required")
        if not isinstance(config, RedisConfig):
            raise ConfigurationError("config must be a RedisConfig instance")
        if not config.url:
            raise ConfigurationError("config.url is required")

        self.stream = stream
        self.config = config
        self._redis: redis_lib.Redis | None = None

    def publish(self, event: StreamBusEvent) -> None:
        if not isinstance(event, StreamBusEvent):
            raise EventValidationError("event must be a StreamBusEvent instance")
        if not event.event_type:
            raise EventValidationError("event.event_type is required")

        self._connect().xadd(self.stream, event.to_dict())

    def _connect(self) -> redis_lib.Redis:
        if self._redis is None:
            self._redis = redis_lib.from_url(
                self.config.url,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
            )
        return self._redis
