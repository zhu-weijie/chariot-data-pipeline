import psycopg2
import structlog
from typing import List, Dict

from config.config import settings
from src.interfaces.loader import Loader

log = structlog.get_logger()


class PostgresLoader(Loader):
    def __init__(self):
        self.db_config = {
            "user": settings.postgres.user,
            "password": settings.postgres.password,
            "host": settings.postgres.host,
            "dbname": settings.postgres.db,
        }
        log.info("PostgreSQL Loader initialized.")

    def _get_connection(self):
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.OperationalError as err:
            log.error("Failed to connect to PostgreSQL", error=str(err))
            raise

    def get_high_water_mark(self) -> int:
        query = "SELECT MAX(movie_id) FROM movies.movies;"
        log.info("Getting high-water mark from PostgreSQL.")

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    result = cursor.fetchone()[0]
                    hwm = result if result is not None else 0
                    log.info("High-water mark retrieved", hwm=hwm)
                    return hwm
        except (psycopg2.Error, TypeError) as err:
            log.error("Failed to get high-water mark", error=str(err))
            return 0

    def _transform_batch(self, batch: List[Dict]) -> List[tuple]:
        transformed = []
        for record in batch:
            genres_list = record.get("genres", "").strip().split("|")
            transformed.append((record["movieId"], record["title"], genres_list))
        return transformed

    def write_batch(self, batch: List[Dict]) -> None:
        transformed_batch = self._transform_batch(batch)
        if not transformed_batch:
            log.warn("Batch is empty after transformation, nothing to write.")
            return

        query = (
            "INSERT INTO movies.movies (movie_id, title, genres) VALUES (%s, %s, %s)"
        )
        log.info("Writing batch to PostgreSQL", num_records=len(transformed_batch))

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    psycopg2.extras.execute_batch(cursor, query, transformed_batch)
                conn.commit()
                log.info("Batch written successfully.")
        except psycopg2.Error as err:
            log.error("Failed to write batch to PostgreSQL", error=str(err))
            raise
