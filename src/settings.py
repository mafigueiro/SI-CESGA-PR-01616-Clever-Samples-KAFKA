import os

import pydantic
from pydantic_settings import BaseSettings as PydanticBaseSettings


ENV_FILE = os.getenv("ENV_FILE", ".env")


class BaseSettings(PydanticBaseSettings):
    """Base class for loading settings.
    The setting variables are loaded from environment settings first, then from the defined env_file.

    Different groups/contexts of settings are created using different classes, that can define an env_prefix which
    will be concatenated to the start of the variable name."""

    class Config:
        env_file = ENV_FILE


class ApplicationSettings(BaseSettings):
    """Settings related with the service/application in general."""

    """Heath check file name."""
    health_check_file_name: str = "/tmp/service.healthcheck"

    """Health check file update period in seconds. The health check is performed by touching a file regularly.
    If the value is <= 0 no file update is done."""
    health_check_period: int = 30

    class Config(BaseSettings.Config):
        env_prefix = "APP_"


class JobSettings(BaseSettings):
    """Settings related with the job this service performs."""

    """Time, in seconds, between one job start and the following."""
    period: int = 5

    """Time to first loop"""
    seconds_to_first_loop: int = 0

    """Time the job takes to be carried"""
    example_job_duration: int = 2

    class Config(BaseSettings.Config):
        env_prefix = "JOB_"


class LoggingSettings(BaseSettings):
    """Settings related with the logging of jobs"""
    level: str = "DEBUG"
    serialize: bool = False

    class Config(BaseSettings.Config):
        env_prefix = "LOG_"


app_settings = ApplicationSettings()
job_settings = JobSettings()
logging_settings = LoggingSettings()
