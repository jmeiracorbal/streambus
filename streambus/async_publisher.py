import logging

from streambus.async_transport import AsyncEventTransport
from streambus.event import StreamBusEvent
from streambus.exceptions import ConfigurationError, EventValidationError

logger = logging.getLogger(__name__)


class AsyncEventPublisher:
    def __init__(self, stream: str, transport: AsyncEventTransport):
        if not stream:
            raise ConfigurationError("stream is required")

        self.stream = stream
        self.transport = transport

    async def publish(self, event: StreamBusEvent) -> None:
        if not isinstance(event, StreamBusEvent):
            raise EventValidationError("event must be a StreamBusEvent instance")
        if not event.event_type:
            raise EventValidationError("event.event_type is required")
        await self.transport.publish(self.stream, event.to_dict())
