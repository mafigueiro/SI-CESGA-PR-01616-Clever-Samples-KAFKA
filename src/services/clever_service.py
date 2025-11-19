from typing import List

from src.logger import logger
from src.models.sample import Sample
from src.services.entities_service import EntitiesService
from src.services.samples_service import SamplesService
from src.utils.normalize import _build_variable_mapping_single
from src.utils.entity_resolver import (
    build_entity_index,
    resolve_entity_path,
)
from src.utils.grouping import group_metrics_by_entity_path
import os
import pandas as pd

class CleverService:
    entities_service = EntitiesService()
    samples_service = SamplesService()
    def __init__(self):

        self.has_root = os.getenv("HAS_ROOT", "false").lower() == "true"
        self.auto_create = os.getenv("AUTO_CREATE", "false").lower() == "true"

        # cargar entidades + generar índice normalizado
        entities = self.entities_service.get_entities()
        self.entity_index = build_entity_index(entities)

        # cache de variables
        self._entity_vars_cache = {}

        logger.info("CleverService inicializado", extra={
            "has_root": self.has_root,
            "auto_create": self.auto_create
        })

    def _get_entity_variables(self, entity_id: str) -> dict:
        if self.auto_create:
            self._entity_vars_cache[entity_id] = {}
            return {}

        if entity_id not in self._entity_vars_cache:
            vars_obj = self.entities_service.get_entity_variables(entity_id)
            self._entity_vars_cache[entity_id] = vars_obj

        return self._entity_vars_cache[entity_id]

    def process_kafka_message(self, normalized: dict) -> None:
        fecha = normalized.get("fecha")

        # NUEVO: parseamos la fecha a datetime (si se puede)
        fecha_dt = None  # type: ignore[assignment]
        if fecha:
            try:
                fecha_dt = pd.to_datetime(fecha)
            except ValueError:
                logger.warning("Fecha con formato inválido, no se crearán Samples con fecha", extra={"fecha": fecha})

        # NUEVO: lista donde iremos acumulando los Samples de este mensaje
        samples: List[Sample] = []

        paths = group_metrics_by_entity_path(normalized)

        for path_parts, metrics in paths.items():
            entity = resolve_entity_path(self.entity_index, path_parts, self.has_root)

            if entity is None:
                logger.warning("Ruta de entidades no resuelta", extra={"path": path_parts})
                continue

            entity_id = entity["entity_id"]

            # 2) Obtener variables de la última entidad (según AUTO_CREATE)
            entity_variables = self._get_entity_variables(entity_id)

            # 3) Construir mapping nombre_variable -> info (variable_id, entity_id, original_name)
            variable_mapping = _build_variable_mapping_single(entity_variables)

            # 4) Comprobar cada variable del mensaje contra el mapping
            incoming_vars = metrics.keys()

            for var in incoming_vars:
                var_lower = str(var).strip().lower()
                info = variable_mapping.get(var_lower)

                if info:
                    logger.info(
                        f"✔ Variable '{var}' está definida para la entidad",
                        extra={
                            "entity_id": entity_id,
                            "entity_name": entity["name"],
                            "variable_id": info.get("variable_id"),
                            "original_name": info.get("original_name"),
                        },
                    )

                    # NUEVO: crear Sample si tenemos fecha válida y valor convertible a float
                    if fecha_dt is not None:
                        valor = metrics[var]
                        try:
                            valor_float = float(valor)
                        except (TypeError, ValueError):
                            logger.warning(
                                "Valor no convertible a float, se ignora para Sample",
                                extra={"variable": var, "valor": valor},
                            )
                        else:
                            sample = Sample(
                                fecha=fecha_dt,
                                variable_id=info["variable_id"],
                                valor=valor_float,
                            )
                            samples.append(sample)
                else:
                    logger.warning(
                        f"✘ Variable '{var}' NO está definida para la entidad",
                        extra={
                            "entity_id": entity_id,
                            "entity_name": entity["name"],
                        },
                    )

            # Log final del mensaje (resumen)
            defined_vars = [v for v in incoming_vars if str(v).strip().lower() in variable_mapping]
            missing_vars = [v for v in incoming_vars if str(v).strip().lower() not in variable_mapping]

            logger.info(
                "Mensaje listo para uso/almacenamiento",
                extra={
                    "fecha": fecha,
                    "path": path_parts,
                    "entity_id": entity_id,
                    "entity_name": entity["name"],
                    "metrics": metrics,
                    "defined_variables": defined_vars,
                    "missing_variables": missing_vars,
                    "auto_create": self.auto_create,
                    "has_root": self.has_root,
                },
            )

        if samples:
            logger.info(
                "Samples construidos a partir del mensaje",
                extra={
                    "num_samples": len(samples),
                    # Ojo: si quieres loguear el contenido entero, puede ser mucho; quita esta línea si molesta
                    "samples_preview": [s.dict() for s in samples[:5]],
                },
            )
            self.samples_service.save_kafka_samples(samples)



