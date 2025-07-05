import random
import sys
import structlog

from src.logging_config import setup_logging
from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.neo4j_loader import Neo4jLoader

SAMPLE_SIZE_PERCENT = 0.05

setup_logging()
log = structlog.get_logger()


class Auditor:
    def __init__(self):
        self.mysql_extractor = MySQLExtractor()
        self.postgres_loader = PostgresLoader()
        self.neo4j_loader = Neo4jLoader()
        self.mismatches = 0

    def _get_all_movie_ids_from_source(self):
        log.info("Fetching all movie IDs from source (MySQL)...")
        query = "SELECT movieId FROM movies ORDER BY movieId ASC"
        with self.mysql_extractor._get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                return [row["movieId"] for row in cursor.fetchall()]

    def _get_postgres_record(self, movie_id: int):
        query = "SELECT movie_id, title, genres FROM movies.movies WHERE movie_id = %s"
        with self.postgres_loader._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (movie_id,))
                row = cursor.fetchone()
                if row:
                    return {"movie_id": row[0], "title": row[1], "genres": row[2]}
        return None

    def _get_neo4j_record(self, movie_id: int):
        query = """
        MATCH (m:Movie {movieId: $movieId})
        OPTIONAL MATCH (m)-[:IN_GENRE]->(g:Genre)
        RETURN m.movieId AS movie_id, m.title AS title, COLLECT(g.name) AS genres
        """
        with self.neo4j_loader._get_session() as session:
            result = session.run(query, movieId=movie_id).single()
            if result:
                return {
                    "movie_id": result["movie_id"],
                    "title": result["title"],
                    "genres": sorted(result["genres"]),
                }
        return None

    def _compare_records(self, source_record, pg_record, neo4j_record):
        movie_id = source_record["movieId"]
        log.info("Auditing record", movie_id=movie_id)

        source_title = source_record["title"]
        source_genres = sorted(source_record.get("genres", "").strip().split("|"))

        if not pg_record:
            log.error("Mismatch found: Record missing in PostgreSQL", movie_id=movie_id)
            self.mismatches += 1
            return
        if source_title != pg_record["title"] or source_genres != sorted(
            pg_record["genres"]
        ):
            log.error(
                "Mismatch found: Data differs in PostgreSQL",
                movie_id=movie_id,
                source=source_record,
                postgres=pg_record,
            )
            self.mismatches += 1

        if not neo4j_record:
            log.error("Mismatch found: Record missing in Neo4j", movie_id=movie_id)
            self.mismatches += 1
            return
        if (
            source_title != neo4j_record["title"]
            or source_genres != neo4j_record["genres"]
        ):
            log.error(
                "Mismatch found: Data differs in Neo4j",
                movie_id=movie_id,
                source=source_record,
                neo4j=neo4j_record,
            )
            self.mismatches += 1

    def run(self):
        log.info("--- Starting Data Integrity Audit ---")

        all_ids = self._get_all_movie_ids_from_source()
        if not all_ids:
            log.error("Source database is empty. Cannot run audit.")
            return

        sample_size = int(len(all_ids) * SAMPLE_SIZE_PERCENT)
        if sample_size == 0:
            sample_size = len(all_ids)
        sample_ids = random.sample(all_ids, k=sample_size)
        log.info(f"Auditing a random sample of {sample_size} records...")

        query_template = (
            "SELECT movieId, title, genres FROM movies WHERE movieId IN ({})"
        )
        placeholders = ", ".join(["%s"] * len(sample_ids))
        query = query_template.format(placeholders)

        with self.mysql_extractor._get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query, sample_ids)
                source_records = cursor.fetchall()

        for src_record in source_records:
            movie_id = src_record["movieId"]
            pg_record = self._get_postgres_record(movie_id)
            neo4j_record = self._get_neo4j_record(movie_id)
            self._compare_records(src_record, pg_record, neo4j_record)

        self.neo4j_loader.close()
        log.info("--- Data Integrity Audit Finished ---")

        if self.mismatches == 0:
            log.info(
                f"✅ Audit PASSED: All {sample_size} sampled records are consistent."
            )
            return True
        else:
            log.error(f"❌ Audit FAILED: Found {self.mismatches} mismatched records.")
            return False


if __name__ == "__main__":
    auditor = Auditor()
    success = auditor.run()
    if not success:
        sys.exit(1)
