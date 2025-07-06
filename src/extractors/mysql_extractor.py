import mysql.connector
import structlog
from typing import List, Dict

from config.config import settings
from src.interfaces.extractor import Extractor

log = structlog.get_logger()


class MySQLExtractor(Extractor):
    def __init__(self):
        self.db_config = {
            "user": settings.mysql.user,
            "password": settings.mysql.password,
            "host": settings.mysql.host,
            "database": settings.mysql.db,
        }
        log.info("MySQL Extractor initialized.")

    def _get_connection(self):
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            log.error("Failed to connect to MySQL", error=str(err))
            raise

    def read_batch(self, batch_size: int, high_water_mark: int) -> List[Dict]:
        query = """
            SELECT movieId, title, genres
            FROM movies
            WHERE movieId > %s
            ORDER BY movieId ASC
            LIMIT %s
        """
        log.info(
            "Reading batch from MySQL",
            batch_size=batch_size,
            high_water_mark=high_water_mark,
        )

        try:
            with self._get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(query, (high_water_mark, batch_size))
                    result = cursor.fetchall()
                    log.info("Batch read successfully", num_records=len(result))
                    return result
        except mysql.connector.Error as err:
            log.error("Failed to read batch from MySQL", error=str(err))
            return []

    def get_next_high_water_mark(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        return batch[-1]["movieId"]
