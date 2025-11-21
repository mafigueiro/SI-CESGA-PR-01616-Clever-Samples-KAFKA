from typing import Dict, List, Any, Optional

from src.models.entity_request import EntityRequest
from src.services.entities_service import EntitiesService
from src.utils.normalize import normalize_entity_key
from src.logger import logger
from src.utils.hierarchy import _load_hierarchy

# Cargamos la jerarquía una vez a nivel de módulo
_HIERARCHY = _load_hierarchy()
_HIER_CHILDREN: Dict[str, List[str]] = _HIERARCHY.get("children", {}) or {}
_HIER_ROOTS: List[str] = _HIERARCHY.get("roots", []) or []


def build_entity_index(entities: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for e in entities:
        name = e.get("name")
        if not name:
            continue
        key = normalize_entity_key(name)
        index[key] = e
    return index


def resolve_entity_by_name(
        index: Dict[str, Dict[str, Any]],
        name: str,
) -> Optional[Dict[str, Any]]:
    key = normalize_entity_key(name)
    return index.get(key)


def resolve_parent_entity(
        index: Dict[str, Dict[str, Any]],
        name: str = None,
) -> Optional[Dict[str, Any]]:
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


def _choose_entity_type_for_creation(
        parent_entity: Optional[Dict[str, Any]],
) -> tuple[str, bool]:
    """
    Decide el 'type' y si es root para una entidad a crear, usando la jerarquía.

    - Si parent_entity es None:
        * Si hay tipos root en la jerarquía: usa el primero.
        * Si no, fallback a 'CASO_DE_USO'.
        * is_root = True.
    - Si parent_entity existe:
        * Busca en la jerarquía los tipos hijos de parent_entity['type'].
        * Si hay hijos: usa el primero.
        * Si no hay: fallback al tipo del padre o 'CENTRO'.
        * is_root = False.
    """
    # Caso root (sin padre)
    if parent_entity is None:
        if _HIER_ROOTS:
            entity_type = _HIER_ROOTS[0]
        else:
            entity_type = "CASO_DE_USO"
            logger.warning(
                "No se encontraron tipos root en la jerarquía. "
                "Usando tipo por defecto 'CASO_DE_USO' para entidad raíz creada automáticamente."
            )
        return entity_type, True

    # Caso entidad hija
    parent_type = str(parent_entity.get("type", "")).strip()
    child_types = _HIER_CHILDREN.get(parent_type, [])

    if child_types:
        entity_type = child_types[0]
        logger.info(
            "Seleccionando tipo de entidad hija desde la jerarquía",
            extra={
                "parent_type": parent_type,
                "chosen_type": entity_type,
                "available_children": child_types,
            },
        )
    else:
        # Fallback: no hay hijos definidos en la jerarquía para este tipo
        entity_type = parent_type or "CENTRO"
        logger.warning(
            "No se encontraron tipos hijos en la jerarquía para el tipo padre. "
            "Usando fallback como tipo de la nueva entidad.",
            extra={
                "parent_type": parent_type,
                "chosen_type": entity_type,
            },
        )

    return entity_type, False


def create_entity(
        parent_entity: Optional[Dict[str, Any]],
        name: str,
) -> Dict[str, Any]:
    """
    Crea una entidad usando Entities API, respetando la jerarquía.

    - parent_entity: entidad padre o None si es raíz.
    - name: nombre de la nueva entidad.

    Usa:
      - Para root: el tipo definido como root en la jerarquía (_HIER_ROOTS[0]),
        o 'CASO_DE_USO' como fallback.
      - Para hijas: el primer tipo hijo definido en la jerarquía para el tipo del padre,
        o el tipo del padre / 'CENTRO' como fallback.

    Devuelve el dict de la entidad creada (resultado de la API).
    """
    entities_service = EntitiesService()
    parent_entity_id = parent_entity.get("entity_id") if parent_entity else None

    entity_type, is_root_flag = _choose_entity_type_for_creation(parent_entity)

    logger.info(
        "AUTO_CREATE: creando entidad a través de Entities API",
        extra={
            "name": name,
            "parent_entity_id": parent_entity_id,
            "type": entity_type,
            "is_root": is_root_flag,
        },
    )

    req = EntityRequest(
        name=name,
        description=f"Entidad auto-creada para '{name}'",
        type=entity_type,
        is_root=is_root_flag,
        attributes={},
        parent_entity_id=parent_entity_id,
        external_id=name,
    )

    create_result = entities_service.create_entity(req)
    result = create_result.get("result") or {}

    if not create_result.get("success", False):
        logger.error(
            "Error creando entidad mediante AUTO_CREATE",
            extra={
                "name": name,
                "parent_entity_id": parent_entity_id,
                "type": entity_type,
                "is_root": is_root_flag,
                "error": create_result.get("error"),
                "status_code": create_result.get("status_code"),
            },
        )

    return result


def resolve_entity_path(
        index: Dict[str, Dict[str, Any]],
        path_parts: tuple[str, ...],
        has_root: bool,
        auto_create: bool,
) -> Optional[tuple[Dict[str, Any], bool]]:
    """
    Resuelve la ruta de entidades:

        ['CasoUso', 'Farmacia1', 'ZonaA'] → entidad final

    Devuelve:
        (ultima_entidad, created_flag)

    donde created_flag = True si se creó alguna entidad durante el proceso.
    """
    resolved: List[Dict[str, Any]] = []
    created_any = False

    for i, name in enumerate(path_parts):
        entity = resolve_entity_by_name(index, name)

        if entity is None:
            if not auto_create:
                # No existe esta entidad y no está permitido crear
                logger.error(
                    "No se encontró entidad en la ruta y AUTO_CREATE está desactivado.",
                    extra={
                        "missing_name": name,
                        "path_parts": path_parts,
                        "step_index": i,
                    },
                )
                return None

            # Crear entidad automáticamente
            parent = resolved[-1] if resolved else None

            if parent is None and not has_root:
                # Si no hay root obligatoria, podemos intentar colgar de una root existente
                parent = resolve_parent_entity(index)

            entity = create_entity(parent_entity=parent, name=name)

            # Actualizamos el índice
            key = normalize_entity_key(name)
            index[key] = entity

            created_any = True

        # Validación de root (la parte fuerte de has_root se gestiona en CleverService._ensure_root_for_path)
        if i == 0 and has_root and not entity.get("is_root"):
            logger.warning(
                "La primera entidad de la ruta no es root",
                extra={"name": name, "entity": entity},
            )
            if not auto_create:
                return None
            # Si has_root y auto_create=True, se espera que CleverService
            # haya manejado la creación de la root antes de llamar aquí.

        resolved.append(entity)

    return (resolved[-1], created_any) if resolved else None
