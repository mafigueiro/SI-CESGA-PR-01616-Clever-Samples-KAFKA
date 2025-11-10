from time import time, sleep
from typing import Callable
from uuid import uuid4

from src.logger import logger


def get_time(seconds_precision=True):
    """Return current time as Unix/Epoch timestamp, in seconds.
    :param seconds_precision: if True, return with seconds precision as integer (default).
                              If False, return with milliseconds precision as floating point number of seconds.
    """
    return time() if not seconds_precision else int(time())


def get_uuid():
    """Return a UUID4 as string"""
    return str(uuid4())


def sleep_until_timestamp(timestamp: int) -> None:
    """Sleeps a number of seconds"""
    time_to_sleep = timestamp - get_time(seconds_precision=False)
    logger.debug(f"Sleeping for {time_to_sleep} seconds.")
    if time_to_sleep > 0:
        sleep(time_to_sleep)


def sleep_until_timestamp_with_callback(
        timestamp: int,
        sleep_period: int = 1,
        on_period_passed_callback: Callable[[], None] = None) -> None:
    """Sleeps a number of seconds until timestamp is reached. Calls on_period_passed_callback
    each sleep_period (starting at time = 0, so it is called at least once)."""
    logger.debug(f"Sleeping for {timestamp - get_time(seconds_precision=False)} seconds.")

    if on_period_passed_callback:
        on_period_passed_callback()

    while get_time(seconds_precision=False) < timestamp:
        sleep(sleep_period)
        if on_period_passed_callback:
            on_period_passed_callback()
