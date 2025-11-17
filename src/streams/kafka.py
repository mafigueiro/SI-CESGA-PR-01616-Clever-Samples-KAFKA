from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from src.config.models import KafkaConfig
from src.logger import logger
from src.streams.base import StreamClient


class KafkaClient(StreamClient):
    """Cliente Kafka basado en confluent-kafka."""

    def __init__(self, cfg: KafkaConfig):
        self.cfg = cfg
        self._consumer = None

    def connect(self) -> None:
        try:
            from confluent_kafka import Consumer  # type: ignore[import]
        except Exception:
            logger.error(
                "confluent-kafka is not installed. Install it with "
                "`pip install confluent-kafka` or change kind in config."
            )
            raise

        conf: Dict[str, Any] = {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "group.id": self.cfg.group_id,
            "auto.offset.reset": "earliest",
        }

        if self.cfg.security_protocol:
            conf["security.protocol"] = self.cfg.security_protocol
        if self.cfg.sasl_mechanism:
            conf["sasl.mechanisms"] = self.cfg.sasl_mechanism
        if self.cfg.sasl_username:
            conf["sasl.username"] = self.cfg.sasl_username
        if self.cfg.sasl_password:
            conf["sasl.password"] = self.cfg.sasl_password
        if self.cfg.ssl_ca_location:
            conf["ssl.ca.location"] = self.cfg.ssl_ca_location

        self._consumer = Consumer(conf)
        self._consumer.subscribe([self.cfg.topic])

        logger.info(
            "KafkaClient connected",
            extra={
                "bootstrap_servers": self.cfg.bootstrap_servers,
                "topic": self.cfg.topic,
                "group_id": self.cfg.group_id,
                "security_protocol": self.cfg.security_protocol,
            },
        )

    def poll(self, timeout_s: float) -> Optional[Tuple[bytes, Dict[str, Any]]]:
        msg = self._consumer.poll(timeout_s)
        if msg is None:
            return None
        if msg.error():
            logger.warning(f"Kafka consumer error: {msg.error()}")
            return None

        headers: Dict[str, Any] = {}
        if msg.headers():
            headers = {
                k: v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else v
                for k, v in msg.headers()
            }

        meta: Dict[str, Any] = {
            "partition": msg.partition(),
            "offset": msg.offset(),
            "headers": headers,
        }
        return msg.value(), meta

    def close(self) -> None:
        try:
            if self._consumer:
                self._consumer.close()
        finally:
            logger.info("KafkaClient closed")
