import multiprocessing
import psycopg2
import structlog

from config.config import settings
from src.logging_config import setup_logging
from src.aggregators.ratings_aggregator import RatingsAggregator

log = structlog.get_logger()


class AggregationDispatcher:
    def __init__(self):
        self.db_config = {
            "user": settings.postgres.user,
            "password": settings.postgres.password,
            "host": settings.postgres.host,
            "dbname": settings.postgres.db,
        }
        self.num_processes = multiprocessing.cpu_count()
        log.info("Aggregation Dispatcher initialized", num_processes=self.num_processes)

    def _get_connection(self):
        return psycopg2.connect(**self.db_config)

    def pre_process_create_batches(self):
        log.info("Starting pre-processing: creating job batches.")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                log.info("Clearing old job and staging data.")
                cursor.execute(
                    "TRUNCATE TABLE jobs.aggregation_batches RESTART IDENTITY;"
                )
                cursor.execute("TRUNCATE TABLE movies.ratings_summary_staging;")

                cursor.execute(
                    "SELECT MIN(movie_id), MAX(movie_id) FROM movies.movies;"
                )
                min_movie_id, max_movie_id = cursor.fetchone()

                log.info("Movie ID range found", min=min_movie_id, max=max_movie_id)

                batch_size = 1000
                for start_id in range(min_movie_id, max_movie_id + 1, batch_size):
                    end_id = start_id + batch_size - 1
                    if end_id > max_movie_id:
                        end_id = max_movie_id

                    insert_query = """
                        INSERT INTO jobs.aggregation_batches (start_movie_id, end_movie_id)
                        VALUES (%s, %s);
                    """
                    cursor.execute(insert_query, (start_id, end_id))

            conn.commit()
            log.info("Successfully created job batches.")
        except Exception as e:
            conn.rollback()
            log.error("Failed during pre-processing", error=str(e))
            raise
        finally:
            conn.close()

    def run_parallel_aggregation(self):
        log.info("Starting parallel aggregation process.")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT batch_id FROM jobs.aggregation_batches WHERE status = 'pending';"
                )
                pending_batches = [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

        if not pending_batches:
            log.warn("No pending batches to process. Exiting.")
            return

        log.info("Distributing tasks to worker pool", num_batches=len(pending_batches))

        with multiprocessing.Pool(processes=self.num_processes) as pool:
            pool.map(worker_process, pending_batches)

        log.info("All worker processes have completed.")

    def finalize_promotion(self):
        log.info("Starting final data promotion.")
        conn = self._get_connection()
        try:
            with conn.cursor() as cursor:
                log.info("Locking tables and promoting data.")

                cursor.execute("TRUNCATE TABLE movies.ratings_summary;")

                cursor.execute(
                    """
                    INSERT INTO movies.ratings_summary (movie_id, average_rating, rating_count)
                    SELECT movie_id, average_rating, rating_count
                    FROM movies.ratings_summary_staging;
                """
                )

                promoted_rows = cursor.rowcount

                conn.commit()
                log.info("Data promotion successful.", promoted_rows=promoted_rows)

        except Exception as e:
            conn.rollback()
            log.error("Failed during data promotion", error=str(e))
            raise
        finally:
            conn.close()


def worker_process(batch_id: int):
    aggregator = RatingsAggregator()
    aggregator.process_batch(batch_id)


def main():
    setup_logging()
    log.info("--- Starting Aggregation Pipeline ---")
    dispatcher = AggregationDispatcher()

    dispatcher.pre_process_create_batches()

    dispatcher.run_parallel_aggregation()

    log.info("--- Aggregation Pipeline Finished ---")


if __name__ == "__main__":
    main()
