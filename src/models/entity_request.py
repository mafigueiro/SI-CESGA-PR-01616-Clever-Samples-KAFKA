"""Entity Request Model
Modelo simple para representar una peticion para crear una entidad
"""

# # Native # #
from datetime import datetime
from typing import Optional

# # Installed # #
from pydantic import Field


__all__ = ("EntityRequest",)

from src.models.base import BaseModel


class EntityRequest(BaseModel):
    """
    Modelo para crear entidades en la Entities API.
    Ajusta los valores por defecto a EntityFields.* si los tienes definidos.
    """
    name: str
    type: str
    is_root: bool = False
    attributes: dict = {}
    parent_entity_id: Optional[str] = None
