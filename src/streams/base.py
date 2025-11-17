from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple


class StreamClient(ABC):
    """
    Interfaz base para clientes de streaming.
    poll() devuelve (payload, meta) o None si no hay mensaje.
    """

    @abstractmethod
    def connect(self) -> None:
        """Conecta con la fuente (Kafka, stdin, etc.)"""

    @abstractmethod
    def poll(self, timeout_s: float) -> Optional[Tuple[bytes, Dict[str, Any]]]:
        """Obtiene un mensaje, o None si no hay."""

    @abstractmethod
    def close(self) -> None:
        """Cierra la conexión / limpia recursos."""
