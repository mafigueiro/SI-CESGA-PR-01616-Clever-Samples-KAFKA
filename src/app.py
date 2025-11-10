"""Service entry point"""
from typing import Union

from src.file_toucher.file_toucher import FileToucher
from src.job.base_job import BaseJob
from src.settings import job_settings, logging_settings, app_settings
from src.logger import logger
from src.utils import get_time, sleep_until_timestamp_with_callback
from src.job.add_numbers_job import AddNumbersJob


class BaseRunCallback:
    """
    Contains callback functions called during the endless loop, making possible to test run().
    Extend it for your purposes (done, for example, in tests/utils.py).
    """

    def on_loop_started(self, loop_start_timestamp: int) -> None:
        """Called when a loop has just started"""
        pass

    def on_loop_finished(self, loop_start_timestamp: int, should_continue: bool):
        """Called when a loop has finished"""

    def update_should_continue(self) -> bool:
        """Called after on_loop_finished(), the returned values is assigned to the variable that is evaluated to
        continue the main loop."""
        return True


def run():
    run_for_real(
        job=AddNumbersJob(),
        callback=None,
        health_check_updater=FileToucher())


def run_for_real(
        job: BaseJob = AddNumbersJob(),
        callback: BaseRunCallback = None,
        health_check_updater: Union[FileToucher, None] = None) -> None:
    """Run the service main loop"""
    logger.info(f"App started with job_settings -> {job_settings}, logging_settings -> {logging_settings}")

    def health_check_callback() -> None:
        if health_check_updater:
            health_check_updater.update()

    health_check_callback()

    if job_settings.seconds_to_first_loop > 0:
        sleep_until_timestamp_with_callback(
            timestamp=get_time(seconds_precision=True) + job_settings.seconds_to_first_loop,
            sleep_period=1,
            on_period_passed_callback=health_check_callback)

    loop_start_timestamp: int = get_time(seconds_precision=True)

    should_continue: bool = True
    while should_continue:
        if callback:
            callback.on_loop_started(loop_start_timestamp)
        try:
            with logger.contextualize(loop_id=loop_start_timestamp):
                job.loop()
                sleep_until_timestamp_with_callback(
                    timestamp=loop_start_timestamp + job_settings.period,
                    sleep_period=1,
                    on_period_passed_callback=health_check_callback)
                loop_start_timestamp = get_time(seconds_precision=True)
        except KeyboardInterrupt:
            logger.info("App exited because of keyboard interrupt.")
            should_continue = False
        if callback:
            callback.on_loop_finished(loop_start_timestamp, should_continue)
            should_continue = callback.update_should_continue()
