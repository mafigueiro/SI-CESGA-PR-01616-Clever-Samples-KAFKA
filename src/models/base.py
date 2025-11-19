"""BASE
Misc base models/variables
"""

# # Installed # #
from pydantic import BaseModel as PydanticBaseModel, ConfigDict

__all__ = (
    "BaseModel",
    "STRING_MAX_LENGTH",
    "UUID_LENGTH",
)

STRING_MAX_LENGTH = 255
"""String limit, bound to database fields"""
UUID_LENGTH = 36
"""UUID string limit, bound to database fields"""


class BaseModel(PydanticBaseModel):
    """Base model for all the Pydantic models used"""

    model_config = ConfigDict(from_attributes=True, str_strip_whitespace=True)

    def model_dump(self, mode: str = "python", include_nulls=False, **kwargs):
        # The model_dump method set exclude_none True by default, unless the new property include_nulls=True
        kwargs["exclude_none"] = not include_nulls
        return super().model_dump(mode=mode, **kwargs)
