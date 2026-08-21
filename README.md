# streambus

[![PyPI version](https://img.shields.io/pypi/v/streambus?color=e8445a&label=pypi)](https://pypi.org/project/streambus/)
[![PyPI downloads](https://img.shields.io/pypi/dm/streambus?color=e8445a)](https://pypi.org/project/streambus/)
[![Python](https://img.shields.io/badge/python-3.11%2B-4584b6?logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-22c55e)](LICENSE)
[![Release](https://github.com/jmeiracorbal/streambus/actions/workflows/release.yml/badge.svg)](https://github.com/jmeiracorbal/streambus/actions/workflows/release.yml)
[![Tests](https://img.shields.io/badge/tests-18%20passed-22c55e)](#development)

Lightweight event bus over Redis Streams. Define typed events, publish from one service, consume in others without boilerplate or framework dependencies.

## Install

```bash
pip install streambus
```

```bash
uv add streambus
```

## Quick start

Define an event:

```python
from dataclasses import dataclass
from streambus import StreamBusEvent

@dataclass(kw_only=True)
class OrderCreated(StreamBusEvent):
    order_id: str
    customer_id: str
    total: str
```

Publish it:

```python
from streambus import EventPublisher, RedisConfig

publisher = EventPublisher(
    stream="orders:events",
    config=RedisConfig(url="redis://localhost:6379"),
)

publisher.publish(
    OrderCreated(
        event_type="order.created",
        order_id="ord_123",
        customer_id="cust_456",
        total="99.90",
    )
)
```

Consume it:

```python
from streambus import EventListener, RedisConfig

def handle_order(event: OrderCreated) -> None:
    print(f"New order {event.order_id}: total {event.total}")

EventListener(
    stream="orders:events",
    group="notifications",
    handler=handle_order,
    event_class=OrderCreated,
    config=RedisConfig(url="redis://localhost:6379"),
).run()
```

## How it works

streambus uses [Redis Streams](https://redis.io/docs/data-types/streams/) for transport: persistent storage, consumer groups, and at-least-once delivery. The library manages group creation, the blocking read loop, message acknowledgement, and automatic reconnection on connection loss.

Each consumer group processes events independently. A message published to a stream reaches every group exactly once.

## Events

All events extend `StreamBusEvent`:

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_type` | `str` | yes | Identifies the event. Convention: `noun.verb` |
| `occurred_at` | `str` | no | ISO 8601 timestamp. Auto-set on creation. |

Subclasses declare only the fields they need. Unknown fields arriving from the stream are silently discarded — each consumer maps what it uses, nothing more.

Both `StreamBusEvent` and its subclasses must use `@dataclass(kw_only=True)`. This is required: the base class has a field with a default (`occurred_at`), and `kw_only=True` prevents the ordering conflict that would otherwise block subclasses from adding required fields.

## Configuration

**RedisConfig**

```python
RedisConfig(
    url="redis://localhost:6379",  # required
    socket_timeout=None,           # None = no timeout (correct for blocking reads)
    socket_connect_timeout=5.0,    # seconds to wait on initial connect
)
```

**EventListener**

```python
EventListener(
    stream="stream:name",     # required
    group="consumer-group",   # required
    handler=my_handler,       # required — callable(event) -> None
    event_class=MyEvent,      # required — must subclass StreamBusEvent
    config=RedisConfig(...),  # required
    consumer=None,            # defaults to "{group}-1"
    batch_size=10,
    block_ms=5000,
    retry_delay=3.0,
)
```

**EventPublisher**

```python
EventPublisher(
    stream="stream:name",    # required
    config=RedisConfig(...), # required
)
```

## Exceptions

| Exception | When |
|---|---|
| `ConfigurationError` | Invalid or missing parameters at construction time |
| `EventValidationError` | `publish()` receives a non-`StreamBusEvent` or empty `event_type` |
| `StreambusError` | Base class for all streambus exceptions |

## Development

```bash
git clone https://github.com/jmeiracorbal/streambus
cd streambus
uv sync --group dev
uv run pytest tests/ -v
```

Run the example (requires Docker):

```bash
cd example
docker compose up -d        # start Redis
uv sync
uv run python consume.py    # terminal 1
uv run python publish.py    # terminal 2
```

## License

MIT
