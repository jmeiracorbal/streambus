import redis as redis_lib

from streambus.config import RedisConfig
from streambus.exceptions import TransportConnectionError


class RedisStreamsTransport:
    def __init__(self, config: RedisConfig):
        self.config = config
        self._client: redis_lib.Redis | None = None

    def _connect(self) -> redis_lib.Redis:
        return redis_lib.from_url(
            self.config.url,
            decode_responses=True,
            socket_timeout=self.config.socket_timeout,
            socket_connect_timeout=self.config.socket_connect_timeout,
        )

    def _get_client(self) -> redis_lib.Redis:
        if self._client is None:
            self._client = self._connect()
        return self._client

    def reconnect(self) -> None:
        self._client = self._connect()

    def publish(self, stream: str, data: dict) -> None:
        try:
            self._get_client().xadd(stream, data)
        except redis_lib.exceptions.ConnectionError as exc:
            raise TransportConnectionError(str(exc)) from exc

    def read(self, stream: str, group: str, consumer: str, batch_size: int, block_ms: int) -> list[tuple[str, dict]]:
        try:
            results = self._get_client().xreadgroup(
                group, consumer, {stream: ">"}, count=batch_size, block=block_ms,
            )
            if not results:
                return []
            return [(msg_id, data) for _stream, messages in results for msg_id, data in messages]
        except redis_lib.exceptions.ConnectionError as exc:
            raise TransportConnectionError(str(exc)) from exc

    def ack(self, stream: str, group: str, msg_id: str) -> None:
        try:
            self._get_client().xack(stream, group, msg_id)
        except redis_lib.exceptions.ConnectionError as exc:
            raise TransportConnectionError(str(exc)) from exc

    def ensure_group(self, stream: str, group: str) -> None:
        try:
            self._get_client().xgroup_create(stream, group, id="0", mkstream=True)
        except redis_lib.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        except redis_lib.exceptions.ConnectionError as exc:
            raise TransportConnectionError(str(exc)) from exc
