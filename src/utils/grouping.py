from collections import defaultdict
from typing import Dict, List, Any


def group_metrics_by_entity_path(normalized: Dict[str, Any]) -> Dict[tuple, Dict[str, Any]]:
    """
    Recibe:
      {
        "fecha": "2024-12-14",
        "farmacia1.zonaA.muertos": 3,
        "farmacia1.zonaA.vacunados": 5,
        "farmacia2.zonaB.infectados": 10
      }

    Devuelve:
      {
        ('farmacia1','zonaA'): {"muertos":3, "vacunados":5},
        ('farmacia2','zonaB'): {"infectados":10}
      }
    """
    grouped = defaultdict(dict)

    for key, value in normalized.items():
        if key == "fecha":
            continue
        if "." not in key:
            continue

        parts = key.split(".")
        *path, variable = parts
        path_tuple = tuple(path)
        grouped[path_tuple][variable] = value

    return grouped
