import logging
import time
from typing import Callable

from streambus.event import StreamBusEvent
from streambus.exceptions import ConfigurationError, TransportConnectionError
from streambus.transport import EventTransport

logger = logging.getLogger(__name__)


class EventListener:
    def __init__(
        self,
        stream: str,
        group: str,
        handler: Callable[[StreamBusEvent], None],
        event_class: type[StreamBusEvent],
        transport: EventTransport,
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

        self.stream = stream
        self.group = group
        self.handler = handler
        self.event_class = event_class
        self.transport = transport
        self.consumer = consumer or f"{group}-1"
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.retry_delay = retry_delay

    def process_one_batch(self) -> int:
        messages = self.transport.read(
            self.stream, self.group, self.consumer, self.batch_size, self.block_ms,
        )
        count = 0
        for msg_id, data in messages:
            try:
                event = self.event_class.from_dict(data)
                self.handler(event)
                self.transport.ack(self.stream, self.group, msg_id)
                count += 1
            except Exception:
                logger.exception("error processing message %s", msg_id)
        return count

    def run(self) -> None:
        self.transport.ensure_group(self.stream, self.group)
        logger.info("[%s] listening on %s", self.group, self.stream)

        while True:
            try:
                self.process_one_batch()
            except TransportConnectionError:
                logger.warning(
                    "[%s] transport connection lost, retrying in %.1fs",
                    self.group,
                    self.retry_delay,
                )
                time.sleep(self.retry_delay)
                self.transport.reconnect()
