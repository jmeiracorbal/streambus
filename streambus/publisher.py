import logging

from streambus.event import StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError
from streambus.transport import EventTransport

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, stream: str, transport: EventTransport):
        if not stream:
            raise ConfigurationError("stream is required")

        self.stream = stream
        self.transport = transport

    def publish(self, event: StreamBusEvent) -> None:
        if not isinstance(event, StreamBusEvent):
            raise EventValidationError("event must be a StreamBusEvent instance")
        if not event.event_type:
            raise EventValidationError("event.event_type is required")
        self.transport.publish(self.stream, event.to_dict())
