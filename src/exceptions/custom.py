"""Exceptions related to the controllers"""
from typing import Union


class NumberException(Exception):
    """The number generate a exception."""

    def __init__(self, message: Union[str, None]):
        self.message = message
