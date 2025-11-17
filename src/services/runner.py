from __future__ import annotations

import signal
from pathlib import Path
from typing import Optional

from src.logger import logger
from src.config.loader import load_streaming_config
from src.services.processor import process_message
from src.streams import client_factory
from src.streams.base import StreamClient


class Runner:
    def __init__(self, config_path: Path, poll_interval_s: float = 1.0):
        self.config_path = config_path
        self.poll_interval_s = poll_interval_s
        self._stop = False
        self._client: Optional[StreamClient] = None

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGINT, self._handle_stop)
        signal.signal(signal.SIGTERM, self._handle_stop)

    def _handle_stop(self, *_):
        self._stop = True
        logger.info("Shutdown signal received, stopping main loop...")

    def run(self) -> None:
        self._setup_signals()

        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found at {self.config_path}. "
                "Mount it via ConfigMap or set APP_CONFIG_PATH."
            )

        cfg = load_streaming_config(self.config_path)
        self._client = client_factory(cfg)
        self._client.connect()

        logger.info(
            "Runner started",
            extra={"config_path": str(self.config_path), "poll_interval_s": self.poll_interval_s},
        )

        try:
            while not self._stop:
                item = self._client.poll(timeout_s=self.poll_interval_s)
                if item is None:
                    continue
                payload, meta = item
                process_message(payload, meta)
        finally:
            if self._client:
                try:
                    self._client.close()
                except Exception:
                    logger.exception("Error closing stream client")
            logger.info("Runner stopped.")
