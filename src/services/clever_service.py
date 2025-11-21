from typing import List, Optional
import os
import pandas as pd

from src.config.models import ServiceConfig
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


class CleverService:
    entities_service = EntitiesService()
    samples_service = SamplesService()

    def __init__(self, service_cfg: Optional["ServiceConfig"] = None):

        if service_cfg is not None:
            # Config venida desde YAML (AppConfig.service)
            self.has_root = bool(service_cfg.has_root)
            logger.info(f"has_root: {self.has_root}")
            self.auto_create = bool(service_cfg.auto_create)
            logger.info(f"auto_create: {self.auto_create}")
        else:
            # Fallback: variables de entorno (como tenías antes)
            self.has_root = os.getenv("HAS_ROOT", "false").lower() == "true"
            self.auto_create = os.getenv("AUTO_CREATE", "false").lower() == "true"

        # cargar entidades + generar índice normalizado
        entities = self.entities_service.get_entities()
        self.entity_index = build_entity_index(entities)

        # cache de variables por entity_id
        self._entity_vars_cache = {}

        logger.info(
            "CleverService inicializado",
            extra={
                "has_root": self.has_root,
                "auto_create": self.auto_create,
            },
        )

    def _get_entity_variables(self, entity_id: str, variable_name: str, entity_created: bool) -> dict:
        """
        Obtiene (y cachea) las variables de una entidad.

        - Si entity_created es True y AUTO_CREATE está activo, crea una variable
          usando el nombre de la variable del mensaje Kafka.
        - Después, si no hay nada en caché para ese entity_id, llama a la API
          para obtener las variables y las guarda en caché.
        """
        # Si la entidad se ha creado en este mensaje y AUTO_CREATE está activo,
        # creamos una variable por defecto con el nombre de la métrica Kafka.
        if entity_created and self.auto_create:
            try:
                logger.info(
                    "Entidad recién creada: creando variable por defecto",
                    extra={"entity_id": entity_id, "variable_name": variable_name},
                )
                self.entities_service.create_variable(entity_id, variable_name)
            except Exception as e:
                logger.error(
                    "Error creando variable por defecto para entidad recién creada",
                    extra={
                        "entity_id": entity_id,
                        "variable_name": variable_name,
                        "error": str(e),
                    },
                )

        # Cache de variables por entity_id
        if entity_id not in self._entity_vars_cache:
            logger.info(
                "Cache MISS de variables, llamando a EntitiesService",
                extra={"entity_id": entity_id},
            )
            vars_obj = self.entities_service.get_entity_variables(entity_id)
            self._entity_vars_cache[entity_id] = vars_obj
        else:
            logger.info(
                "Cache HIT de variables para entidad",
                extra={"entity_id": entity_id},
            )

        return self._entity_vars_cache[entity_id]

    def process_kafka_message(self, normalized: dict) -> None:
        fecha = normalized.get("fecha")

        # Parseamos la fecha a datetime (si se puede)
        fecha_dt = None  # type: ignore[assignment]
        if fecha:
            try:
                fecha_dt = pd.to_datetime(fecha)
            except ValueError:
                logger.warning(
                    "Fecha con formato inválido, no se crearán Samples con fecha",
                    extra={"fecha": fecha},
                )

        samples: List[Sample] = []

        # paths: Dict[tuple[str], Dict[str, Any]]
        paths = group_metrics_by_entity_path(normalized)

        for path_parts, metrics in paths.items():
            # resolve_entity_path ahora devuelve Optional[tuple[dict, bool]]
            result = resolve_entity_path(
                self.entity_index,
                path_parts,
                self.has_root,
                self.auto_create,
            )

            if result is None:
                logger.warning("Ruta de entidades no resuelta", extra={"path": path_parts})
                continue

            entity, created = result

            entity_id = entity["entity_id"]
            entity_name = entity["name"]

            incoming_vars = list(metrics.keys())
            logger.info(
                "Variables del mensaje para la entidad",
                extra={
                    "entity_id": entity_id,
                    "entity_name": entity_name,
                    "incoming_vars": incoming_vars,
                },
            )

            # Como por diseño solo debería haber 1 variable por path,
            # este bucle normalmente sólo tendrá una iteración.
            entity_variables = None
            variable_mapping = None

            for var in incoming_vars:
                variable_name = str(var)              # nombre tal cual viene del mensaje Kafka
                var_lower = variable_name.strip().lower()

                # 1) Obtener variables de la entidad (y crear variable si la entidad es nueva)
                if entity_variables is None:
                    entity_variables = self._get_entity_variables(
                        entity_id=entity_id,
                        variable_name=variable_name,
                        entity_created=created,
                    )
                    logger.info(f"Variables de la entidad: {entity_variables}")
                    variable_mapping = _build_variable_mapping_single(entity_variables)

                info = (variable_mapping or {}).get(var_lower)

                if info:
                    logger.info(
                        f"✔ Variable '{variable_name}' está definida para la entidad",
                        extra={
                            "entity_id": entity_id,
                            "entity_name": entity_name,
                            "variable_id": info.get("variable_id"),
                            "original_name": info.get("original_name"),
                        },
                    )

                    # crear Sample si tenemos fecha válida y valor convertible a float
                    if fecha_dt is not None:
                        valor = metrics[var]
                        try:
                            valor_float = float(valor)
                        except (TypeError, ValueError):
                            logger.warning(
                                "Valor no convertible a float, se ignora para Sample",
                                extra={"variable": variable_name, "valor": valor},
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
                        f"✘ Variable '{variable_name}' NO está definida para la entidad",
                        extra={
                            "entity_id": entity_id,
                            "entity_name": entity_name,
                        },
                    )

            # Resumen por mensaje
            variable_mapping = variable_mapping or {}
            defined_vars = [
                v
                for v in incoming_vars
                if str(v).strip().lower() in variable_mapping
            ]
            missing_vars = [
                v
                for v in incoming_vars
                if str(v).strip().lower() not in variable_mapping
            ]

            logger.info(
                "Mensaje listo para uso/almacenamiento",
                extra={
                    "fecha": fecha,
                    "path": path_parts,
                    "entity_id": entity_id,
                    "entity_name": entity_name,
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
                    "samples_preview": [s.model_dump() for s in samples[:5]],
                },
            )
            self.samples_service.save_kafka_samples(samples)
