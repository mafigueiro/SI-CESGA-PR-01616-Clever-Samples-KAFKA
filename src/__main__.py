"""
__main__.py

Project: SI-CESGA-PR-01616-Clever-Samples-KAFKA

Maintainer Marco Alvarezx (mafigueiro@gradiant.org)

Copyright (c) 2025 Centro Tecnolóxico de Telecomunicacións de Galicia (GRADIANT)
All Rights Reserved
"""

from src.app import run
from src.logger import logger


if __name__ == "__main__":
    logger.info("Starting periodic service main loop from __main__")
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user (KeyboardInterrupt).")
    except Exception:
        logger.exception("Service crashed with an unexpected error")
        raise
