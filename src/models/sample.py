"""Sample Model
Modelo simple para representar una muestra de CSV
"""

# # Native # #
from datetime import datetime

# # Installed # #
from pydantic import Field


__all__ = ("Sample",)

from src.models.base import BaseModel


class Sample(BaseModel):
    """Muestra procesada desde CSV"""
    fecha: datetime = Field(..., description="Fecha de la muestra")
    variable_id: str = Field(..., description="ID de la variable")
    valor: float = Field(..., description="Valor de la muestra")