import asyncio
from dataclasses import dataclass

import fakeredis.aioredis
import pytest

from streambus import AsyncEventListener, AsyncRedisStreamsTransport, RedisConfig, StreamBusEvent
from streambus.exceptions import ConfigurationError


@dataclass(kw_only=True)
class SampleEvent(StreamBusEvent):
    document: str


@pytest.mark.asyncio
async def test_recovers_pending_message_after_handler_failure() -> None:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    transport = AsyncRedisStreamsTransport(RedisConfig(url="redis://localhost:6379"))
    transport._client = client
    await transport.ensure_group("documents", "conversion")

    await client.xadd("documents", SampleEvent(event_type="document.changed", document="one.pdf").to_dict())

    async def failing_handler(event: SampleEvent) -> None:
        raise RuntimeError("conversion failed")

    failing = AsyncEventListener(
        stream="documents", group="conversion", consumer="worker-a",
        handler=failing_handler, event_class=SampleEvent, transport=transport,
        pending_idle_ms=1, block_ms=0,
    )
    assert await failing.process_one_batch() == 0
    assert (await client.xpending("documents", "conversion"))["pending"] == 1

    received: list[SampleEvent] = []

    async def successful_handler(event: SampleEvent) -> None:
        received.append(event)

    successful = AsyncEventListener(
        stream="documents", group="conversion", consumer="worker-b",
        handler=successful_handler, event_class=SampleEvent, transport=transport,
        pending_idle_ms=1, block_ms=0,
    )
    await asyncio.sleep(0.02)
    assert await successful.process_one_batch() == 1
    assert [event.document for event in received] == ["one.pdf"]
    assert (await client.xpending("documents", "conversion"))["pending"] == 0
    await client.aclose()


@pytest.mark.asyncio
async def test_processes_new_entries_when_no_pending_eligible() -> None:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    transport = AsyncRedisStreamsTransport(RedisConfig(url="redis://localhost:6379"))
    transport._client = client
    await transport.ensure_group("documents", "conversion")
    await client.xadd("documents", SampleEvent(event_type="document.changed", document="two.pdf").to_dict())

    received: list[SampleEvent] = []

    async def handler(event: SampleEvent) -> None:
        received.append(event)

    listener = AsyncEventListener(
        stream="documents", group="conversion", handler=handler,
        event_class=SampleEvent, transport=transport, block_ms=0,
    )
    assert await listener.process_one_batch() == 1
    assert [event.document for event in received] == ["two.pdf"]
    await client.aclose()


@pytest.mark.parametrize("overrides", [
    {"batch_size": 0},
    {"block_ms": -1},
    {"pending_idle_ms": 0},
])
def test_rejects_invalid_async_listener_parameters(overrides: dict) -> None:
    transport = AsyncRedisStreamsTransport(RedisConfig(url="redis://localhost:6379"))

    async def handler(event: SampleEvent) -> None:
        pass

    with pytest.raises(ConfigurationError):
        AsyncEventListener(
            stream="documents", group="conversion", handler=handler,
            event_class=SampleEvent, transport=transport, **overrides,
        )
