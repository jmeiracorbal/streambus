from dataclasses import dataclass, asdict, field, fields as dc_fields
from datetime import datetime, timezone


@dataclass(kw_only=True)
class StreamBusEvent:
    event_type: str
    occurred_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, str]:
        return {k: str(v) for k, v in asdict(self).items()}

    @classmethod
    def from_dict(cls, data: dict[str, str]) -> "StreamBusEvent":
        known = {f.name for f in dc_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})
