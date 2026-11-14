"""
Listen for client events and print them.

Usage:
    python consume.py
"""
import logging
from dataclasses import dataclass

from streambus import EventListener, RedisConfig, RedisStreamsTransport, StreamBusEvent

logging.basicConfig(level=logging.INFO, format="%(message)s")


@dataclass(kw_only=True)
class ClientEvent(StreamBusEvent):
    client_id: str = ""
    name: str = ""
    slug: str = ""
    is_active: str = ""


def handle(event: ClientEvent) -> None:
    print(f"  client_id : {event.client_id}")
    print(f"  name      : {event.name}")
    print(f"  slug      : {event.slug}")
    print(f"  is_active : {event.is_active}")
    print(f"  occurred  : {event.occurred_at}")
    print()


EventListener(
    stream="mynps:clients:events",
    group="example",
    handler=handle,
    event_class=ClientEvent,
    transport=RedisStreamsTransport(RedisConfig(url="redis://localhost:6379")),
).run()
