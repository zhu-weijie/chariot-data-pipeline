import psycopg2
from psycopg2 import extras
import structlog
from typing import List, Dict, Tuple

from config.config import settings
from src.interfaces.loader import Loader

log = structlog.get_logger()


class PostgresRatingsLoader(Loader):
    def __init__(self):
        self.db_config = {
            "user": settings.postgres.user,
            "password": settings.postgres.password,
            "host": settings.postgres.host,
            "dbname": settings.postgres.db,
        }
        log.info("PostgreSQL Ratings Loader initialized.")

    def _get_connection(self):
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.OperationalError as err:
            log.error("Failed to connect to PostgreSQL", error=str(err))
            raise

    def get_high_water_mark(self) -> Tuple[int, int]:
        query = "SELECT MAX(user_id), MAX(movie_id) FROM movies.ratings WHERE user_id = (SELECT MAX(user_id) FROM movies.ratings);"
        log.info("Getting ratings high-water mark from PostgreSQL.")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()
                    hwm = (result[0] or 0, result[1] or 0)
                    log.info("Ratings high-water mark retrieved", hwm=hwm)
                    return hwm
        except (psycopg2.Error, TypeError) as err:
            log.error("Failed to get ratings high-water mark", error=str(err))
            return (0, 0)

    def write_batch(self, batch: List[Dict]) -> None:
        transformed_batch = [
            (rec["userId"], rec["movieId"], rec["rating"], rec["timestamp"])
            for rec in batch
        ]

        query = "INSERT INTO movies.ratings (user_id, movie_id, rating, timestamp) VALUES (%s, %s, %s, %s)"
        log.info(
            "Writing ratings batch to PostgreSQL", num_records=len(transformed_batch)
        )

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    extras.execute_batch(cursor, query, transformed_batch)
                conn.commit()
                log.info("Ratings batch written successfully.")
        except psycopg2.Error as err:
            log.error("Failed to write ratings batch to PostgreSQL", error=str(err))
            raise
