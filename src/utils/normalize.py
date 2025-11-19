from typing import Dict, Any
from src.logger import logger

def normalize_entity_key(name: str) -> str:
    """
    Normaliza un nombre de entidad para comparaciones.
    Ejemplo:
      'Farmacia 1' -> 'farmacia1'
      'FARMACIA CESGA' -> 'farmaciacesga'
    """
    return name.strip().lower().replace(" ", "")


def _build_variable_mapping_single( entities_result: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Construye un mapeo de nombres de columnas a variable_id y entity_id
    para la nueva respuesta que contiene una sola entidad y sus variables directamente.
    """
    variable_mapping = {}

    # Accedemos al resultado
    result = entities_result.get("result", {})
    variables = result.get("variables", [])

    if not variables or not isinstance(variables, list):
        logger.warning("No se encontraron variables en la respuesta")
        return variable_mapping

    for variable in variables:
        if not variable or not isinstance(variable, dict):
            logger.warning("Variable vacía o inválida encontrada")
            continue

        variable_id = variable.get("variable_id", "")
        variable_name = ""
        configuration = variable.get("configuration", {})

        if isinstance(configuration, dict):
            node_mapping = configuration.get("node_mapping", {})
            variable_name = node_mapping.get("source_name", "")

        if not variable_name:
            variable_name = variable.get("opc_ua_name", "")

        if not variable_name or not variable_id:
            logger.warning(f"Variable con datos incompletos: name='{variable_name}', id='{variable_id}'")
            continue

        if not variable_name.strip() or not variable_id.strip():
            logger.warning(f"Variable con datos vacíos (solo espacios): name='{variable_name}', id='{variable_id}'")
            continue

        variable_name_clean = variable_name.strip()
        variable_id_clean = variable_id.strip()
        variable_name_lower = variable_name_clean.lower()

        entity_id = variable.get("entity_id", "")

        variable_mapping[variable_name_lower] = {
            "variable_id": variable_id_clean,
            "entity_id": entity_id,
            "original_name": variable_name_clean
        }

    return variable_mapping
