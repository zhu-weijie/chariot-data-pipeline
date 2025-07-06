import pandas as pd
import psycopg2
from psycopg2 import extras
import structlog

from config.config import settings

log = structlog.get_logger()


class RatingsAggregator:
    def __init__(self):
        self.db_config = {
            "user": settings.postgres.user,
            "password": settings.postgres.password,
            "host": settings.postgres.host,
            "dbname": settings.postgres.db,
        }
        log.info("Ratings Aggregator initialized.")

    def _get_connection(self):
        return psycopg2.connect(**self.db_config)

    def _update_batch_status(self, conn, batch_id: int, status: str):
        update_query = (
            "UPDATE jobs.aggregation_batches SET status = %s WHERE batch_id = %s"
        )
        with conn.cursor() as cursor:
            cursor.execute(update_query, (status, batch_id))
        conn.commit()
        log.info("Updated batch status", batch_id=batch_id, status=status)

    def process_batch(self, batch_id: int):
        log.info("Starting to process batch", batch_id=batch_id)

        fetch_query = "SELECT movie_id, rating FROM movies.ratings WHERE movie_id BETWEEN %s AND %s"

        conn = None
        try:
            conn = self._get_connection()

            self._update_batch_status(conn, batch_id, "processing")

            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT start_movie_id, end_movie_id FROM jobs.aggregation_batches WHERE batch_id = %s",
                    (batch_id,),
                )
                start_id, end_id = cursor.fetchone()

            log.info(
                "Fetching ratings for batch",
                batch_id=batch_id,
                start_id=start_id,
                end_id=end_id,
            )
            df = pd.read_sql_query(fetch_query, conn, params=(start_id, end_id))

            if df.empty:
                log.warn(
                    "No ratings found for this batch. Marking as complete.",
                    batch_id=batch_id,
                )
                self._update_batch_status(conn, batch_id, "complete")
                return

            log.info(
                "Aggregating ratings for batch", batch_id=batch_id, num_ratings=len(df)
            )
            aggregation = df.groupby("movie_id")["rating"].agg(["mean", "count"])
            aggregation.rename(
                columns={"mean": "average_rating", "count": "rating_count"},
                inplace=True,
            )
            aggregation["average_rating"] = aggregation["average_rating"].round(5)

            log.info(
                "Writing aggregated results to staging table",
                batch_id=batch_id,
                num_movies=len(aggregation),
            )
            with conn.cursor() as cursor:
                insert_data = [
                    (int(index), float(row["average_rating"]), int(row["rating_count"]))
                    for index, row in aggregation.iterrows()
                ]
                insert_query = "INSERT INTO movies.ratings_summary_staging (movie_id, average_rating, rating_count) VALUES %s"
                extras.execute_values(cursor, insert_query, insert_data)

            self._update_batch_status(conn, batch_id, "complete")
            log.info("Successfully processed batch", batch_id=batch_id)

        except Exception as e:
            log.error("Failed to process batch", batch_id=batch_id, error=str(e))
            if conn:
                conn.rollback()

            status_conn = None
            try:
                status_conn = self._get_connection()
                self._update_batch_status(status_conn, batch_id, "failed")
            except Exception as status_e:
                log.error(
                    "CRITICAL: Failed to even mark batch as failed!",
                    batch_id=batch_id,
                    status_error=str(status_e),
                )
            finally:
                if status_conn:
                    status_conn.close()
            raise
        finally:
            if conn:
                conn.close()
