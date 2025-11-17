from src.config.models import StreamingConfig
from src.streams.base import StreamClient
from src.streams.kafka import KafkaClient
from src.streams.noop import NoopClient
from src.streams.stdin import StdinClient


def client_factory(cfg: StreamingConfig) -> StreamClient:
    if cfg.kind == "kafka" and cfg.kafka:
        return KafkaClient(cfg.kafka)
    if cfg.kind == "stdin":
        return StdinClient()
    return NoopClient()
