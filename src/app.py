"""High-level entrypoint for the streaming service."""

from __future__ import annotations

import os
from pathlib import Path

from src.logger import logger
from src.services.runner import Runner


def run() -> None:
    """
    Entry point used by __main__.py.
    - Lee la ruta del archivo de config desde APP_CONFIG_PATH (o /config/config.yaml).
    - Lee el intervalo de poll desde APP_POLL_INTERVAL_S (por defecto 1.0 s).
    """
    config_path = Path(os.getenv("APP_CONFIG_PATH", "src/config/config.yaml"))
    poll_interval = float(os.getenv("APP_POLL_INTERVAL_S", "1.0"))

    logger.info(
        "Starting streaming service",
        extra={
            "config_path": str(config_path),
            "poll_interval_s": poll_interval,
        },
    )

    runner = Runner(config_path=config_path, poll_interval_s=poll_interval)
    runner.run()
