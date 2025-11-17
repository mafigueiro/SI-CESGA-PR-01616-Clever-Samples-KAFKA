from src.app import run
from src.logger import logger


if __name__ == "__main__":
    logger.info("Starting streaming service from __main__...")
    try:
        run()
    except KeyboardInterrupt:
        logger.info("Service stopped by user (KeyboardInterrupt).")
    except Exception:
        logger.exception("Service crashed with an unexpected error")
        raise
