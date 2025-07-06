import structlog
from src.logging_config import setup_logging
from src.extractors.mysql_extractor import MySQLExtractor

setup_logging()
log = structlog.get_logger()
extractor = MySQLExtractor()
batch_size = 5
high_water_mark = 0

log.info("--- Running first extraction batch ---")
batch1 = extractor.read_batch(batch_size=batch_size, high_water_mark=high_water_mark)
print(batch1)

if batch1:
    high_water_mark = batch1[-1]["movieId"]
    log.info("New high_water_mark", mark=high_water_mark)

    log.info("--- Running second extraction batch ---")
    batch2 = extractor.read_batch(
        batch_size=batch_size, high_water_mark=high_water_mark
    )
    print(batch2)
