import logging
import time
from typing import Callable

import redis as redis_lib

from streambus.config import RedisConfig
from streambus.event import StreamBusEvent
from streambus.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class EventListener:
    def __init__(
        self,
        stream: str,
        group: str,
        handler: Callable[[StreamBusEvent], None],
        event_class: type[StreamBusEvent],
        config: RedisConfig,
        consumer: str | None = None,
        batch_size: int = 10,
        block_ms: int = 5000,
        retry_delay: float = 3.0,
    ):
        if not stream:
            raise ConfigurationError("stream is required")
        if not group:
            raise ConfigurationError("group is required")
        if not callable(handler):
            raise ConfigurationError("handler must be callable")
        if not (isinstance(event_class, type) and issubclass(event_class, StreamBusEvent)):
            raise ConfigurationError("event_class must be a subclass of StreamBusEvent")
        if not isinstance(config, RedisConfig):
            raise ConfigurationError("config must be a RedisConfig instance")
        if not config.url:
            raise ConfigurationError("config.url is required")

        self.stream = stream
        self.group = group
        self.handler = handler
        self.event_class = event_class
        self.config = config
        self.consumer = consumer or f"{group}-1"
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.retry_delay = retry_delay

    def run(self) -> None:
        r = self._connect()
        self._ensure_group(r)
        logger.info("[%s] listening on %s", self.group, self.stream)

        while True:
            try:
                results = r.xreadgroup(
                    self.group,
                    self.consumer,
                    {self.stream: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )
                for _stream, messages in (results or []):
                    for msg_id, data in messages:
                        try:
                            event = self.event_class.from_dict(data)
                            self.handler(event)
                            r.xack(self.stream, self.group, msg_id)
                        except Exception:
                            logger.exception("error processing message %s", msg_id)
            except redis_lib.exceptions.ConnectionError:
                logger.warning(
                    "[%s] redis connection lost, retrying in %.1fs",
                    self.group,
                    self.retry_delay,
                )
                time.sleep(self.retry_delay)
                r = self._connect()

    def _connect(self) -> redis_lib.Redis:
        return redis_lib.from_url(
            self.config.url,
            decode_responses=True,
            socket_timeout=self.config.socket_timeout,
            socket_connect_timeout=self.config.socket_connect_timeout,
        )

    def _ensure_group(self, r: redis_lib.Redis) -> None:
        try:
            r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
