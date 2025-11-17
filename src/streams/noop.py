from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

from src.logger import logger
from src.streams.base import StreamClient


class NoopClient(StreamClient):
    """Cliente vacío: no consume nada, solo hace sleep."""

    def connect(self) -> None:
        logger.warning("NoopClient: kind=none. Service will not consume any data.")

    def poll(self, timeout_s: float) -> Optional[Tuple[bytes, Dict[str, Any]]]:
        time.sleep(timeout_s)
        return None

    def close(self) -> None:
        logger.info("NoopClient closed.")
