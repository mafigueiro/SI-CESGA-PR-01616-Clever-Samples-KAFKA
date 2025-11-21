from __future__ import annotations

import csv
import io
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from src.config.models import ServiceConfig
from src.logger import logger
from src.services.clever_service import CleverService

DATE_FIELD_CANDIDATES = ["fecha", "date", "dia", "día"]  # por si cambian nombres en el futuro


_clever_service: Optional[CleverService] = None


def init_clever_service(service_cfg: ServiceConfig) -> None:
    """
    Inicializa una instancia global de CleverService usando
    la configuración proveniente del YAML (service_cfg).
    """
    global _clever_service
    _clever_service = CleverService(service_cfg=service_cfg)


def _get_clever_service() -> CleverService:
    """
    Devuelve la instancia global de CleverService si existe.
    Si no se ha inicializado (por tests, scripts, etc.), crea
    una instancia usando variables de entorno (comportamiento antiguo).
    """
    global _clever_service
    if _clever_service is None:
        return CleverService()
    return _clever_service

def _parse_date_field(row: Dict[str, Any]) -> str:
    """
    Intenta localizar el campo de fecha en la fila y normalizarlo a 'YYYY-MM-DD' (string).
    Si no puede, lo deja como está.
    """
    for key in row.keys():
        key_norm = key.strip().lower()
        if key_norm in DATE_FIELD_CANDIDATES:
            raw = str(row[key]).strip()
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(raw, fmt).date().isoformat()
                except ValueError:
                    continue
            # si no encaja ningún formato, lo devolvemos tal cual
            return raw
    # si no se encuentra ningún campo de fecha, no tocamos nada
    return ""


def _coerce_numbers(row: Dict[str, Any], date_key: str | None) -> Dict[str, Any]:
    """
    Convierte a número (int/float) todos los campos salvo el de fecha,
    cuando sea posible.
    """
    result: Dict[str, Any] = {}
    for key, value in row.items():
        if key == date_key:
            result[key] = value
            continue
        if value is None:
            result[key] = None
            continue

        text = str(value).strip()
        if text == "":
            result[key] = None
            continue

        # int o float
        try:
            if "." in text.replace(",", "."):
                result[key] = float(text.replace(",", "."))
            else:
                result[key] = int(text)
        except ValueError:
            result[key] = text

    return result


def _iter_csv_records(text: str) -> Iterable[Dict[str, Any]]:
    """
    Interpreta el payload como CSV/TSV:
    - Autodetecta delimitador.
    - Usa la primera línea como cabecera.
    """
    # probamos delimitadores frecuentes
    candidates = [",", ";", "\t"]
    best_reader = None
    best_count = 0

    for delim in candidates:
        try:
            f = io.StringIO(text)
            reader = list(csv.DictReader(f, delimiter=delim))
            if reader and len(reader[0].keys()) > best_count:
                best_reader = reader
                best_count = len(reader[0].keys())
        except Exception:
            continue

    if not best_reader:
        # no se pudo parsear como CSV
        return []

    return best_reader


def _normalize_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normaliza una fila:
    - detecta fecha
    - convierte métricas a número si puede
    """
    # normalizamos claves (strip)
    row_norm = {k.strip(): v for k, v in row.items()}

    # localizar campo fecha y normalizarlo
    date_value = _parse_date_field(row_norm)
    date_key: str | None = None
    if date_value:
        # intenta encontrar la clave original que contenía la fecha
        for k in row_norm.keys():
            if k.strip().lower() in DATE_FIELD_CANDIDATES:
                date_key = k
                row_norm[k] = date_value
                break

    # convertir métricas a número
    row_norm = _coerce_numbers(row_norm, date_key)

    return row_norm


def _handle_records(records: Iterable[Dict[str, Any]]) -> None:
    """
    Aquí podrías, por ejemplo:
    - enviar cada registro a Clever
    - guardarlo en una base de datos
    De momento solo logueamos los datos normalizados.
    """

    clever_service = _get_clever_service()
    for record in records:
        normalized = _normalize_record(record)
        logger.info(
            "Streaming record",
            extra={
                "record": normalized,
                "columns": list(normalized.keys()),
            },
        )
        clever_service.process_kafka_message(normalized)



def process_message(payload: bytes, meta: Dict[str, Any]) -> None:
    """
    Procesa un mensaje del stream.
    Soporta:
    - JSON: objeto único o lista de objetos
    - CSV/TSV: texto con cabecera + filas (como tu Excel exportado)
    """
    # 1) Intentar texto utf-8
    try:
        text = payload.decode("utf-8")
    except Exception:
        logger.info(
            "Received non-utf8 payload",
            extra={"meta": meta, "size": len(payload)},
        )
        return

    # 2) Intentar JSON
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            records: List[Dict[str, Any]] = [obj]
        elif isinstance(obj, list):
            records = [r for r in obj if isinstance(r, dict)]
        else:
            logger.warning(
                "JSON format not supported for streaming record",
                extra={"meta": meta, "type": type(obj).__name__},
            )
            return

        _handle_records(records)
        return
    except json.JSONDecodeError:
        # no es JSON, seguimos con CSV
        pass

    # 3) Intentar CSV/TSV
    rows = list(_iter_csv_records(text))
    if not rows:
        logger.warning(
            "Payload could not be parsed as JSON or CSV",
            extra={"meta": meta, "sample": text[:200]},
        )
        return

    _handle_records(rows)
