import mysql.connector
import structlog
from typing import List, Dict, Tuple

from config.config import settings
from src.interfaces.extractor import Extractor

log = structlog.get_logger()


class MySQLRatingsExtractor(Extractor):
    def __init__(self):
        self.db_config = {
            "user": settings.mysql.user,
            "password": settings.mysql.password,
            "host": settings.mysql.host,
            "database": settings.mysql.db,
        }
        log.info("MySQL Ratings Extractor initialized.")

    def _get_connection(self):
        try:
            return mysql.connector.connect(**self.db_config)
        except mysql.connector.Error as err:
            log.error("Failed to connect to MySQL", error=str(err))
            raise

    def read_batch(
        self, batch_size: int, high_water_mark: Tuple[int, int]
    ) -> List[Dict]:
        last_user_id, last_movie_id = high_water_mark

        query = """
            SELECT userId, movieId, rating, timestamp
            FROM ratings
            WHERE (userId, movieId) > (%s, %s)
            ORDER BY userId ASC, movieId ASC
            LIMIT %s
        """
        log.info(
            "Reading ratings batch from MySQL",
            batch_size=batch_size,
            high_water_mark=f"({last_user_id}, {last_movie_id})",
        )

        try:
            with self._get_connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(query, (last_user_id, last_movie_id, batch_size))
                    result = cursor.fetchall()
                    log.info("Ratings batch read successfully", num_records=len(result))
                    return result
        except mysql.connector.Error as err:
            log.error("Failed to read ratings batch from MySQL", error=str(err))
            return []

    def get_next_high_water_mark(self, batch: List[Dict]) -> Tuple[int, int]:
        if not batch:
            return (0, 0)
        last_record = batch[-1]
        return (last_record["userId"], last_record["movieId"])
