from typing import Dict, List, Any, Optional
from src.utils.normalize import normalize_entity_key


def build_entity_index(entities: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Construye un índice:
      key normalizada -> entidad
    """
    index = {}
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


def resolve_entity_path(index: Dict[str, Dict[str, Any]], path_parts: List[str], has_root: bool) -> Optional[Dict[str, Any]]:
    """
    Resuelve toda la ruta:
      ['CasoUso', 'Farmacia1', 'ZonaA'] → entidad final 'ZonaA'
    """
    resolved = []

    for i, name in enumerate(path_parts):
        entity = resolve_entity_by_name(index, name)
        if entity is None:
            return None

        if i == 0 and has_root and not entity.get("is_root"):
            return None

        resolved.append(entity)

    return resolved[-1]  # última entidad
