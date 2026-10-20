from dataclasses import dataclass

import fakeredis

from streambus import EventListener, StreamBusEvent
from streambus.config import RedisConfig
from streambus.transports.redis_streams import RedisStreamsTransport


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    document: str


def test_recovers_pending_message_after_handler_failure() -> None:
    client = fakeredis.FakeRedis(decode_responses=True)
    transport = RedisStreamsTransport(RedisConfig(url="redis://localhost:6379"))
    transport._client = client
    transport.ensure_group("documents", "conversion")

    client.xadd("documents", SampleEvent(event_type="document.changed", document="one.pdf").to_dict())
    failed = EventListener(
        stream="documents", group="conversion", consumer="worker-a",
        handler=lambda event: (_ for _ in ()).throw(RuntimeError("conversion failed")),
        event_class=SampleEvent, transport=transport, pending_idle_ms=1,
    )
    assert failed.process_one_batch() == 0
    assert client.xpending("documents", "conversion")["pending"] == 1

    recovered = []
    successful = EventListener(
        stream="documents", group="conversion", consumer="worker-b",
        handler=recovered.append, event_class=SampleEvent,
        transport=transport, pending_idle_ms=1,
    )
    # Time-free deterministic claim: zero idle would bypass the delay, but the library
    # deliberately requires a positive delay, so emulate a sufficiently idle record.
    import time
    time.sleep(0.01)
    assert successful.process_one_batch() == 1
    assert [event.document for event in recovered] == ["one.pdf"]
    assert client.xpending("documents", "conversion")["pending"] == 0


def test_processes_new_entries_when_no_pending_eligible() -> None:
    client = fakeredis.FakeRedis(decode_responses=True)
    transport = RedisStreamsTransport(RedisConfig(url="redis://localhost:6379"))
    transport._client = client
    transport.ensure_group("documents", "conversion")
    client.xadd("documents", SampleEvent(event_type="document.changed", document="two.pdf").to_dict())
    received = []
    listener = EventListener(
        stream="documents", group="conversion", handler=received.append,
        event_class=SampleEvent, transport=transport, block_ms=0,
    )
    assert listener.process_one_batch() == 1
    assert received[0].document == "two.pdf"
