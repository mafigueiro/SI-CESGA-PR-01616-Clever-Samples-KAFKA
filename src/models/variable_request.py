from pydantic import BaseModel


class VariableNodeMapping(BaseModel):
    """
    Mapeo de la variable a nivel de origen (nombre en Kafka, tipo, unidad, etc.)
    """
    source_name: str                      # nombre de la variable en el mensaje Kafka
    type: str = "string to int"           # por defecto: "string to int"
    unit: str = "int"                     # por defecto: "int"


class VariableConfiguration(BaseModel):
    """
    Configuración de la variable en la Entities API.
    """
    description: str                      # descripción legible ("Vacunados", "Muertos", etc.)
    node_mapping: VariableNodeMapping
    readable_name: str                    # nombre legible para UI / usuario


class VariableRequest(BaseModel):
    """
    Cuerpo para crear una variable en /entities/{entity_id}/variables

    Ejemplo JSON:

    {
        "type": "DATALOGGER",
        "variable_type_name": "DB_PARAM",
        "configuration": {
            "description": "Vacunados",
            "node_mapping": {
                "source_name": "vacunados",
                "type": "string to int",
                "unit": "int"
            },
            "readable_name": "vacunados"
        },
        "opc_ua_name": "maval_param"
    }
    """
    type: str = "DATALOGGER"
    variable_type_name: str = "DB_PARAM"
    configuration: VariableConfiguration
    opc_ua_name: str = "maval_param"
