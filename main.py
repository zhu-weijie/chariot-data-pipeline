import structlog
from src.logging_config import setup_logging

setup_logging()

log = structlog.get_logger()


def main():
    log.info("Chariot Data Pipeline starting...")
    log.info("Chariot Data Pipeline finished.")


if __name__ == "__main__":
    main()
