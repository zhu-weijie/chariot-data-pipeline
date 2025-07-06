import structlog
from src.logging_config import setup_logging
from src.extractors.mysql_ratings_extractor import MySQLRatingsExtractor
from src.loaders.neo4j_ratings_loader import Neo4jRatingsLoader

setup_logging()
log = structlog.get_logger()
ratings_extractor = MySQLRatingsExtractor()
ratings_loader = Neo4jRatingsLoader()

try:
    log.info("--- Running mini ETL pipeline for Neo4j Ratings ---")

    hwm = ratings_loader.get_high_water_mark()
    log.info("Initial ratings high-water mark", hwm=hwm)

    batch = ratings_extractor.read_batch(batch_size=20, high_water_mark=hwm)

    if batch:
        ratings_loader.write_batch(batch)
        log.info("--- ETL run finished. Verifying... ---")

        new_hwm = ratings_loader.get_high_water_mark()
        log.info("New ratings high-water mark", hwm=new_hwm)
        assert new_hwm > hwm, "Neo4j Ratings high-water mark did not increase!"
        log.info("Verification successful!")
    else:
        log.info("No new ratings data to process.")
finally:
    ratings_loader.close()
