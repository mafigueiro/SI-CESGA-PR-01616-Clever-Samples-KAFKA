"""
Application data model: Number
"""
from typing import Optional

from pydantic import BaseModel, Field


class Number(BaseModel):
    """An integer number"""
    value: int = Field(description="The number value", example=3)


class NumberUpdate(BaseModel):
    """An update to an integer number"""
    value: Optional[int] = Field(description="The number value", example=3)
