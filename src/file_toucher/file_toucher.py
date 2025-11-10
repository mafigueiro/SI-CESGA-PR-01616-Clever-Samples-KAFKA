"""Implements a class that can touch a file, creating it if does not exist.
This is used by a health check script."""
from pathlib import Path

from src.logger import logger
from src.settings import app_settings
from src.utils import get_time


class FileToucher:
    """Touches a file given by the settings."""

    def __init__(self):
        self.last_timestamp_file_was_touched = 0

    def update(self):
        """Touches the file if needed. This function must be called regularly in order to touch the file."""
        if app_settings.health_check_period <= 0:
            return
        current_timestamp = get_time()
        if current_timestamp >= self.last_timestamp_file_was_touched + app_settings.health_check_period:
            self.touch_file()
            self.last_timestamp_file_was_touched = current_timestamp

    def touch_file(self):
        logger.debug(f"Touching file {app_settings.health_check_file_name}.")
        Path(app_settings.health_check_file_name).touch(exist_ok=True)
