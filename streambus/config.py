from dataclasses import dataclass, field


@dataclass
class RedisConfig:
    url: str
    socket_timeout: float | None = field(default=None)
    socket_connect_timeout: float | None = field(default=5.0)
