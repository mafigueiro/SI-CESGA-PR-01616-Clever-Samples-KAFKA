from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from src.models.base import BaseModel


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    group_id: str = "stream-service"
    security_protocol: str = "PLAINTEXT"  # or "SASL_SSL"
    sasl_mechanism: Optional[str] = None  # e.g. "PLAIN"
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None  # path to CA if TLS


@dataclass(frozen=True)
class StreamingConfig:
    # kind: "kafka" | "stdin" | "none"
    kind: str
    kafka: Optional[KafkaConfig] = None


class ServiceConfig(BaseModel):
    has_root: bool = False
    auto_create: bool = False


class AppConfig(BaseModel):
    streaming: StreamingConfig
    service: ServiceConfig