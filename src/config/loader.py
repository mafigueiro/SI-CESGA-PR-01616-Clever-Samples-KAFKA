from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

from src.logger import logger
from src.config.models import KafkaConfig, StreamingConfig, ServiceConfig, AppConfig


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a YAML mapping at top level.")
    return data


def load_streaming_config(path: Path) -> StreamingConfig:
    """
    Lee el archivo YAML y construye un StreamingConfig.
    Estructura esperada:

    kind: kafka | stdin | none

    kafka:
      bootstrap_servers: "kafka-1:9092"
      topic: "mi-topic"
      group_id: "mi-grupo"
      security_protocol: "PLAINTEXT"  # o "SASL_SSL"
      sasl_mechanism: "PLAIN"
      sasl_username: "user"
      sasl_password: "pass"
      ssl_ca_location: "/ruta/ca.crt"
    """
    logger.info("Loading streaming config", extra={"path": str(path)})
    data = _load_yaml(path)

    kind = str(data.get("kind", "none")).lower()

    if kind == "kafka":
        k = data.get("kafka", {}) or {}
        cfg = KafkaConfig(
            bootstrap_servers=str(k["bootstrap_servers"]),
            topic=str(k["topic"]),
            group_id=str(k.get("group_id", "stream-service")),
            security_protocol=str(k.get("security_protocol", "PLAINTEXT")),
            sasl_mechanism=k.get("sasl_mechanism"),
            sasl_username=k.get("sasl_username"),
            sasl_password=k.get("sasl_password"),
            ssl_ca_location=k.get("ssl_ca_location"),
        )
        return StreamingConfig(kind="kafka", kafka=cfg)

    if kind == "stdin":
        return StreamingConfig(kind="stdin")

    logger.warning("Config kind is 'none' or unknown; service will not consume data.")
    return StreamingConfig(kind="none")


def load_app_config(path: Path) -> AppConfig:
    """
    Carga tanto la configuración de streaming como la del servicio (has_root, auto_create)
    desde el mismo YAML.
    """
    logger.info("Loading app config", extra={"path": str(path)})
    data = _load_yaml(path)

    # Reutilizamos la lógica de streaming
    streaming_cfg = load_streaming_config(path)

    # Sección 'service'
    service_data = data.get("service", {}) or {}

    service_cfg = ServiceConfig(
        has_root=bool(service_data.get("has_root", False)),
        auto_create=bool(service_data.get("auto_create", False)),
    )

    return AppConfig(streaming=streaming_cfg, service=service_cfg)