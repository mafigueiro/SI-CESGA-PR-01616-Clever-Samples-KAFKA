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

from src.utils.hierarchy import _load_hierarchy
from src.utils.grouping import group_metrics_by_entity_path
from src.models.entity_request import EntityRequest


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
            self.has_root = os.getenv("HAS_ROOT", "false").lower() == "true"
            self.auto_create = os.getenv("AUTO_CREATE", "false").lower() == "true"

        # cargar entidades + generar índice normalizado
        entities = self.entities_service.get_entities()
        self.entity_index = build_entity_index(entities)

        self._entity_vars_cache = {}
        hierarchy = _load_hierarchy()
        self.hierarchy_raw = hierarchy["raw"]
        self.hierarchy_children = hierarchy["children"]
        self.hierarchy_parents = hierarchy["parents"]
        self.hierarchy_roots = hierarchy["roots"]
        self.hierarchy_type_levels = hierarchy["levels"]
        self.hierarchy_max_depth = hierarchy["max_depth"]

        logger.info(
            "CleverService inicializado",
            extra={
                "has_root": self.has_root,
                "auto_create": self.auto_create,
            },
        )

    # --------------------------------------------------------------------- #
    #  Root handling
    # --------------------------------------------------------------------- #
    def _ensure_root_for_path(self, path_parts: tuple[str, ...]) -> bool:
        """
        Asegura que, si has_root=True, la primera entidad del path sea root.

        - Si has_root=False: no hace nada y devuelve True.
        - Si has_root=True:
          * Si existe una entidad root con ese nombre: OK.
          * Si NO existe y AUTO_CREATE=False: log de error claro y devuelve False.
          * Si NO existe y AUTO_CREATE=True: crea root, recarga cache y devuelve True.

        NOTA: aquí asumimos que el nombre de la root es la primera parte del path.
        """
        if not self.has_root:
            return True

        if not path_parts:
            logger.error(
                "Path vacío recibido con has_root=True, no se puede determinar entidad root"
            )
            return False

        root_candidate = str(path_parts[0]).strip()
        if not root_candidate:
            logger.error(
                "Nombre de entidad root vacío en el path",
                extra={"path": path_parts},
            )
            return False

        # Buscar una entidad con ese nombre y is_root = True
        root_found = False
        for ent in self.entity_index.values():
            name = str(ent.get("name", "")).strip()
            if name.lower() == root_candidate.lower() and ent.get("is_root", False):
                root_found = True
                break

        if root_found:
            logger.info(
                "Entidad root encontrada para el path",
                extra={"root_name": root_candidate, "path": path_parts},
            )
            return True

        # No hay entidad root con ese nombre
        if not self.auto_create:
            logger.error(
                "No hay entidad root para este caso y AUTO_CREATE está desactivado. "
                "La primera entidad del mensaje debería ser una entidad con is_root=True.",
                extra={
                    "root_candidate": root_candidate,
                    "path": path_parts,
                    "auto_create": self.auto_create,
                    "has_root": self.has_root,
                },
            )
            return False

        try:
            logger.info(
                "AUTO_CREATE activo y no existe entidad root. Creando entidad root...",
                extra={
                    "root_name": root_candidate,
                    "path": path_parts,
                },
            )

            if self.hierarchy_roots:
                root_type = self.hierarchy_roots[0]
            else:
                # Fallback si por algún motivo no tenemos jerarquía:
                root_type = "CASO_DE_USO"
                logger.warning(
                    "No se han encontrado tipos root en la jerarquía. "
                    "Usando tipo por defecto 'CASO_DE_USO'.",
                    extra={"path": path_parts},
                )
            # Construimos un EntityRequest de tipo CASO_DE_USO, is_root=True
            # Ajusta el type si en tu Entities API se usa otro valor para la root.
            root_request = EntityRequest(
                name=root_candidate,
                type=root_type,
                is_root=True,
                attributes={},
                external_id=root_candidate,
                parent_entity_id=None,
            )

            create_result = self.entities_service.create_entity(root_request)

            if not create_result.get("success", False):
                logger.error(
                    "Fallo creando entidad root automáticamente",
                    extra={
                        "root_name": root_candidate,
                        "path": path_parts,
                        "error": create_result.get("error"),
                        "status_code": create_result.get("status_code"),
                    },
                )
                return False

            logger.info(
                "Entidad root creada correctamente",
                extra={
                    "root_name": root_candidate,
                    "path": path_parts,
                    "result": create_result.get("result"),
                },
            )

            # Recargar cache de entidades obligatoriamente
            entities = self.entities_service.get_entities(force_refresh=True)
            self.entity_index = build_entity_index(entities)
            logger.info(
                "Cache de entidades recargada tras crear entidad root",
                extra={"entity_count": len(entities)},
            )

            return True

        except Exception as e:
            logger.error(
                "Error inesperado creando entidad root automáticamente",
                extra={"root_name": root_candidate, "path": path_parts, "error": str(e)},
            )
            return False

    # --------------------------------------------------------------------- #
    #  Variables de entidad
    # --------------------------------------------------------------------- #
    def _get_entity_variables(self, entity_id: str, variable_name: str, entity_created: bool) -> dict:
        """
        Obtiene (y cachea) las variables de una entidad.

        - Si entity_created es True y AUTO_CREATE está activo, crea una variable
          usando el nombre de la variable del mensaje Kafka.
        - Después, si no hay nada en caché para ese entity_id, llama a la API
          para obtener las variables y las guarda en caché.
        """
        if entity_created and self.auto_create:
            try:
                logger.info(
                    "Entidad recién creada: creando variable por defecto",
                    extra={"entity_id": entity_id, "variable_name": variable_name},
                )
                self.entities_service.create_variable(entity_id, variable_name)
                if entity_id in self._entity_vars_cache:
                    del self._entity_vars_cache[entity_id]
            except Exception as e:
                logger.error(
                    "Error creando variable por defecto para entidad recién creada",
                    extra={
                        "entity_id": entity_id,
                        "variable_name": variable_name,
                        "error": str(e),
                    },
                )

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

    # --------------------------------------------------------------------- #
    #  Procesado del mensaje Kafka
    # --------------------------------------------------------------------- #
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
            # 1) Si has_root=True, aseguramos que la primera entidad del path sea root
            if not self._ensure_root_for_path(path_parts):
                continue

            # 2) Resolver la ruta completa de entidades (root + subentidades)
            result = resolve_entity_path(
                self.entity_index,
                path_parts,
                self.has_root,
                self.auto_create,
            )

            if result is None:
                # Si hemos llegado aquí, la parte de root ya estaba controlada; aquí el fallo
                # será por subentidades intermedias.
                if not self.auto_create:
                    logger.error(
                        "No se ha podido resolver la ruta de subentidades y AUTO_CREATE está desactivado. "
                        "No se crearán subentidades automáticamente.",
                        extra={
                            "path": path_parts,
                            "auto_create": self.auto_create,
                            "has_root": self.has_root,
                        },
                    )
                else:
                    logger.error(
                        "No se ha podido resolver la ruta de subentidades incluso con AUTO_CREATE activo.",
                        extra={
                            "path": path_parts,
                            "auto_create": self.auto_create,
                            "has_root": self.has_root,
                        },
                    )
                continue

            entity, created = result

            # 🔁 Si se ha creado una entidad nueva (root o subentidad), recargamos cache de entidades
            if created:
                try:
                    logger.info(
                        "Entidad creada durante la resolución de ruta, recargando cache de entidades",
                        extra={"path": path_parts, "entity_id": entity.get("entity_id")},
                    )
                    entities = self.entities_service.get_entities(force_refresh=True)
                    self.entity_index = build_entity_index(entities)
                    logger.info(
                        "Cache de entidades recargada tras creación",
                        extra={"entity_count": len(entities)},
                    )
                except Exception as e:
                    logger.error(
                        "Error recargando cache de entidades tras creación",
                        extra={"error": str(e)},
                    )

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

                # 3) Obtener variables de la entidad (y crear variable si la entidad es nueva)
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
                    # Mensajes distintos según AUTO_CREATE
                    if not self.auto_create:
                        logger.warning(
                            "Variable no encontrada en la entidad y AUTO_CREATE está desactivado. "
                            "No se creará la variable automáticamente.",
                            extra={
                                "entity_id": entity_id,
                                "entity_name": entity_name,
                                "variable_name": variable_name,
                            },
                        )
                    else:
                        logger.warning(
                            f"✘ Variable '{variable_name}' NO está definida para la entidad. "
                            "AUTO_CREATE está activo, se intentará crear la variable.",
                            extra={
                                "entity_id": entity_id,
                                "entity_name": entity_name,
                            },
                        )

                        # 🔧 AUTO_CREATE: crear variables que falten aunque la entidad ya exista
                        try:
                            logger.info(
                                "AUTO_CREATE activo: creando variable faltante",
                                extra={
                                    "entity_id": entity_id,
                                    "entity_name": entity_name,
                                    "variable_name": variable_name,
                                },
                            )
                            self.entities_service.create_variable(entity_id, variable_name)

                            # invalidar cache de variables de esta entidad
                            if entity_id in self._entity_vars_cache:
                                del self._entity_vars_cache[entity_id]

                            # volver a cargar variables y mapping
                            entity_variables = self._get_entity_variables(
                                entity_id=entity_id,
                                variable_name=variable_name,
                                entity_created=False,  # ya creada la entidad
                            )
                            variable_mapping = _build_variable_mapping_single(entity_variables)
                            info_created = variable_mapping.get(var_lower)

                            # si ahora ya existe, creamos también el Sample
                            if info_created and fecha_dt is not None:
                                valor = metrics[var]
                                try:
                                    valor_float = float(valor)
                                except (TypeError, ValueError):
                                    logger.warning(
                                        "Valor no convertible a float tras crear variable, se ignora",
                                        extra={"variable": variable_name, "valor": valor},
                                    )
                                else:
                                    sample = Sample(
                                        fecha=fecha_dt,
                                        variable_id=info_created["variable_id"],
                                        valor=valor_float,
                                    )
                                    samples.append(sample)

                        except Exception as e:
                            logger.error(
                                "Error en AUTO_CREATE al crear variable faltante",
                                extra={
                                    "entity_id": entity_id,
                                    "entity_name": entity_name,
                                    "variable_name": variable_name,
                                    "error": str(e),
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
