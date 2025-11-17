from __future__ import annotations

import select
import sys
from typing import Any, Dict, Optional, Tuple

from src.logger import logger
from src.streams.base import StreamClient


class StdinClient(StreamClient):
    """Cliente de pruebas: lee líneas desde stdin."""

    def connect(self) -> None:
        logger.info("StdinClient connected. Type lines and press Enter.")

    def poll(self, timeout_s: float) -> Optional[Tuple[bytes, Dict[str, Any]]]:
        r, _, _ = select.select([sys.stdin], [], [], timeout_s)
        if not r:
            return None
        line = sys.stdin.readline()
        if not line:
            return None
        return line.encode("utf-8"), {"source": "stdin"}

    def close(self) -> None:
        logger.info("StdinClient closed.")
