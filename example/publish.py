"""
Send test client events to the Redis stream.

Usage:
    python publish.py                   # sends 3 predefined events
    python publish.py "My Client" my-client
"""
import sys
import uuid
from datetime import datetime, timezone

import redis

STREAM = "mynps:clients:events"
r = redis.from_url("redis://localhost:6379")


def publish(name: str, slug: str, is_active: bool = True) -> bytes:
    payload = {
        "event_type": "client.updated",
        "client_id": str(uuid.uuid4()),
        "name": name,
        "slug": slug,
        "is_active": str(is_active),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    return r.xadd(STREAM, payload)


if __name__ == "__main__":
    if len(sys.argv) == 3:
        events = [(sys.argv[1], sys.argv[2])]
    else:
        events = [
            ("Acme Corp", "acme-corp"),
            ("Beta Ltd", "beta-ltd"),
            ("Gamma SA", "gamma-sa"),
        ]

    for name, slug in events:
        msg_id = publish(name, slug)
        print(f"→ {name}  [{msg_id.decode()}]")
