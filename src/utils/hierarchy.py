# src/utils/hierarchy.py
from __future__ import annotations

from typing import Any, Dict, List
from collections import deque

from src.logger import logger
from src.services.entities_service import EntitiesService

# Caché global interna del módulo
_HIERARCHY_CACHE: Dict[str, Any] | None = None


def _load_hierarchy() -> Dict[str, Any]:
    """
    Carga y procesa la jerarquía de tipos de entidad desde Entities API.

    - Solo llama a la API la primera vez (usa caché en memoria).
    - Devuelve un dict con:
        {
          "raw": <jerarquía cruda>,
          "children": { type -> [children_types] },
          "parents": { type -> [parent_types] },
          "roots": [root_types],
          "levels": { type -> level (0=root) },
          "max_depth": int
        }
    """
    global _HIERARCHY_CACHE

    # Si ya está en caché, la devolvemos tal cual
    if _HIERARCHY_CACHE is not None:
        return _HIERARCHY_CACHE

    entities_service = EntitiesService()
    try:
        resp = entities_service.get_hierarchy()
    except Exception as e:
        logger.error(
            "Error llamando a get_hierarchy en EntitiesService",
            extra={"error": str(e)},
        )
        _HIERARCHY_CACHE = {
            "raw": {},
            "children": {},
            "parents": {},
            "roots": [],
            "levels": {},
            "max_depth": 0,
        }
        return _HIERARCHY_CACHE

    if not resp.get("success", False):
        logger.warning(
            "No se pudo obtener jerarquía de entidades (success=False)",
            extra={
                "status_code": resp.get("status_code"),
                "error": resp.get("error"),
            },
        )
        _HIERARCHY_CACHE = {
            "raw": {},
            "children": {},
            "parents": {},
            "roots": [],
            "levels": {},
            "max_depth": 0,
        }
        return _HIERARCHY_CACHE

    hierarchy = resp.get("result", {}) or {}
    if not isinstance(hierarchy, dict):
        logger.error(
            "Formato inesperado de jerarquía: se esperaba un dict",
            extra={"type": type(hierarchy).__name__},
        )
        _HIERARCHY_CACHE = {
            "raw": {},
            "children": {},
            "parents": {},
            "roots": [],
            "levels": {},
            "max_depth": 0,
        }
        return _HIERARCHY_CACHE

    # --- Construimos children y parents ---
    children_map: dict[str, set[str]] = {}
    parents_map: dict[str, set[str]] = {}

    for type_name, info in hierarchy.items():
        type_name = str(type_name)
        children = info.get("children", []) or []
        children_map.setdefault(type_name, set())
        for child in children:
            child_str = str(child)
            children_map[type_name].add(child_str)
            parents_map.setdefault(child_str, set()).add(type_name)

    all_types = set(hierarchy.keys())
    all_children = set()
    for ch_set in children_map.values():
        all_children |= ch_set

    # Roots = tipos que no son hijos de nadie
    root_types = list(all_types - all_children)

    # --- BFS para niveles ---
    type_levels: dict[str, int] = {}
    queue: deque[str] = deque()

    for root in root_types:
        type_levels[root] = 0
        queue.append(root)

    while queue:
        current = queue.popleft()
        current_level = type_levels[current]
        for child in children_map.get(current, []):
            if child not in type_levels:
                type_levels[child] = current_level + 1
                queue.append(child)

    max_depth = max(type_levels.values()) + 1 if type_levels else 0

    children_dict = {t: sorted(list(children_map.get(t, []))) for t in all_types}
    parents_dict = {t: sorted(list(parents_map.get(t, []))) for t in all_types}

    _HIERARCHY_CACHE = {
        "raw": hierarchy,
        "children": children_dict,
        "parents": parents_dict,
        "roots": root_types,
        "levels": type_levels,
        "max_depth": max_depth,
    }

    logger.info(
        "Jerarquía de entidades procesada y cacheada",
        extra={
            "roots": root_types,
            "type_levels": type_levels,
            "max_depth": max_depth,
        },
    )

    return _HIERARCHY_CACHE
