from typing import Dict, List, Any, Optional

from src.models.entity_request import EntityRequest
from src.services.entities_service import EntitiesService
from src.utils.normalize import normalize_entity_key
from src.logger import logger  # asegúrate de tener este import


def build_entity_index(entities: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for e in entities:
        name = e.get("name")
        if not name:
            continue
        key = normalize_entity_key(name)
        index[key] = e
    return index


def resolve_entity_by_name(index: Dict[str, Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    key = normalize_entity_key(name)
    return index.get(key)

def resolve_parent_entity(index: Dict[str, Dict[str, Any]], name: str = None) -> Optional[Dict[str, Any]]:
    """
    Devuelve la entidad raíz (aquella con is_root = True).
    Ignora el parámetro 'name' porque ahora buscamos siempre la raíz.

    Args:
        index: Diccionario normalizado de entidades.
        name: No se usa, mantenido por compatibilidad.

    Returns:
        La entidad raíz o None si no existe.
    """
    for entity in index.values():
        if entity.get("is_root"):
            return entity

    return None

def create_entity(
        parent_entity: Optional[Dict[str, Any]],
        name: str,
) -> Dict[str, Any]:
    """
    Función ilustrativa para AUTO_CREATE.
    Aquí deberías llamar realmente a la Entities API para crear la entidad.

    - parent_entity: entidad padre o None si es raíz.
    - name: nombre de la nueva entidad.

    Devuelve un dict que represente la entidad creada.
    """
    entities_service = EntitiesService()
    parent_entity_id = parent_entity.get("entity_id") if parent_entity else None
    logger.info(
        "AUTO_CREATE: creando entidad ilustrativa",
        extra={
            "name": name,
            "parent_entity_id": parent_entity_id,
        },
    )

    # EJEMPLO: entidad "fake" solo para desarrollo.
    # Sustituye esto por el resultado real de tu API.
    req = EntityRequest(
        name = name,
        description=  f"Entidad auto-creada para '{name}'",
        type= "CENTRO",
        is_root= parent_entity is None,
        attributes= {},
        parent_entity_id= parent_entity_id,
    )

    return entities_service.create_entity(req).get("result")


def resolve_entity_path(
        index: Dict[str, Dict[str, Any]],
        path_parts: tuple[str],
        has_root: bool,
        auto_create: bool,
) -> Optional[tuple[Dict[str, Any], bool]]:
    """
    Resuelve la ruta de entidades:

        ['CasoUso', 'Farmacia1', 'ZonaA'] → entidad final

    Devuelve:
        (ultima_entidad, created_flag)

    where created_flag = True si se creó alguna entidad durante el proceso.
    """
    resolved: List[Dict[str, Any]] = []
    created_any = False  # <- NUEVO

    for i, name in enumerate(path_parts):

        entity = resolve_entity_by_name(index, name)

        if entity is None:
            if not auto_create:
                return None

            # Crear entidad automáticamente
            parent = resolved[-1] if resolved else None

            if parent is None and not has_root:
                parent = resolve_parent_entity(index)

            entity = create_entity(parent_entity=parent, name=name)

            # Actualizamos el índice
            key = normalize_entity_key(name)
            index[key] = entity

            created_any = True  # <- MARCAMOS QUE SE CREÓ UNA ENTIDAD

        # Validación de root si no estamos creando
        if i == 0 and has_root and not entity.get("is_root"):
            logger.warning(
                "La primera entidad de la ruta no es root",
                extra={"name": name, "entity": entity},
            )
            if not auto_create:
                return None

        resolved.append(entity)

    # DEVOLVER entidad final + flag de creación
    return (resolved[-1], created_any) if resolved else None

