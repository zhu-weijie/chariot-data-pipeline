import structlog
from src.logging_config import setup_logging
from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader

setup_logging()
log = structlog.get_logger()
extractor = MySQLExtractor()
loader = PostgresLoader()

log.info("--- Running mini ETL pipeline for PostgreSQL ---")

hwm = loader.get_high_water_mark()
log.info("Initial high-water mark", hwm=hwm)

batch = extractor.read_batch(batch_size=10, high_water_mark=hwm)

if batch:
    loader.write_batch(batch)
    log.info("--- ETL run finished. Verifying... ---")

    new_hwm = loader.get_high_water_mark()
    log.info("New high-water mark", hwm=new_hwm)
    assert new_hwm > hwm, "High-water mark did not increase!"
    log.info("Verification successful!")
else:
    log.info("No new data to process.")
