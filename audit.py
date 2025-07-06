import random
import sys
import structlog
import numpy as np

from src.logging_config import setup_logging
from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.neo4j_loader import Neo4jLoader
from src.loaders.neo4j_ratings_loader import Neo4jRatingsLoader

SAMPLE_SIZE_PERCENT = 0.05

setup_logging()
log = structlog.get_logger()


class Auditor:
    def __init__(self):
        self.mysql_extractor = MySQLExtractor()
        self.postgres_loader = PostgresLoader()
        self.neo4j_loader = Neo4jLoader()
        self.neo4j_ratings_loader = Neo4jRatingsLoader()
        self.mismatches = 0
        self.aggregation_mismatches = 0
        self.neo4j_ratings_mismatches = 0

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

    def _get_raw_ratings_for_movie(self, movie_id: int):
        query = "SELECT rating FROM ratings WHERE movieId = %s"
        with self.mysql_extractor._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (movie_id,))
                return [row[0] for row in cursor.fetchall()]

    def _get_postgres_summary_record(self, movie_id: int):
        query = "SELECT movie_id, average_rating, rating_count FROM movies.ratings_summary WHERE movie_id = %s"
        with self.postgres_loader._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (movie_id,))
                row = cursor.fetchone()
                if row:
                    return {
                        "movie_id": row[0],
                        "average_rating": float(row[1]),
                        "rating_count": row[2],
                    }
        return None

    def _compare_aggregation(self, movie_id):
        log.info("Auditing aggregation for record", movie_id=movie_id)

        raw_ratings = self._get_raw_ratings_for_movie(movie_id)
        if not raw_ratings:
            log.warn(
                "No ratings in source for movie, skipping aggregation audit.",
                movie_id=movie_id,
            )
            return

        expected_avg = round(float(np.mean(raw_ratings)), 5)
        expected_count = len(raw_ratings)

        summary_record = self._get_postgres_summary_record(movie_id)

        if not summary_record:
            log.error(
                "Aggregation Mismatch: Record missing in summary table",
                movie_id=movie_id,
            )
            self.aggregation_mismatches += 1
            return

        if (
            expected_count != summary_record["rating_count"]
            or expected_avg != summary_record["average_rating"]
        ):
            log.error(
                "Aggregation Mismatch: Data differs in summary table",
                movie_id=movie_id,
                expected={"avg": expected_avg, "count": expected_count},
                found={
                    "avg": summary_record["average_rating"],
                    "count": summary_record["rating_count"],
                },
            )
            self.aggregation_mismatches += 1

    def _get_neo4j_rating(self, user_id: int, movie_id: int):
        query = """
        MATCH (:User {userId: $userId})-[r:RATED]->(:Movie {movieId: $movieId})
        RETURN r.rating AS rating, r.timestamp AS timestamp
        """
        with self.neo4j_ratings_loader._get_session() as session:
            result = session.run(query, userId=user_id, movieId=movie_id).single()
            if result:
                return {"rating": result["rating"], "timestamp": result["timestamp"]}
        return None

    def run(self):
        log.info("--- Starting Data Integrity Audit ---")

        log.info("Fetching all ratings keys from source (MySQL)...")
        query = "SELECT userId, movieId FROM ratings ORDER BY userId, movieId"
        with self.mysql_extractor._get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                all_keys = cursor.fetchall()

        if not all_keys:
            log.error("Source ratings table is empty. Cannot run audit.")
            return

        sample_size = int(len(all_keys) * SAMPLE_SIZE_PERCENT)
        if sample_size == 0:
            sample_size = min(len(all_keys), 100)
        sample_keys = random.sample(all_keys, k=sample_size)
        log.info(f"Auditing a random sample of {sample_size} ratings...")

        key_map_str = ", ".join(
            [f"({k['userId']},{k['movieId']})" for k in sample_keys]
        )

        query_template = "SELECT userId, movieId, rating, timestamp FROM ratings WHERE (userId, movieId) IN ({})"
        query = query_template.format(key_map_str)
        with self.mysql_extractor._get_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                source_records = cursor.fetchall()

        for src_rating_record in source_records:
            user_id = src_rating_record["userId"]
            movie_id = src_rating_record["movieId"]

            neo4j_rating_record = self._get_neo4j_rating(user_id, movie_id)
            if not neo4j_rating_record:
                log.error(
                    "Mismatch: Rating missing in Neo4j",
                    user_id=user_id,
                    movie_id=movie_id,
                )
                self.neo4j_ratings_mismatches += 1
            else:
                source_rating = float(src_rating_record["rating"])
                if source_rating != neo4j_rating_record["rating"]:
                    log.error(
                        "Mismatch: Rating value differs in Neo4j",
                        user_id=user_id,
                        movie_id=movie_id,
                    )
                    self.neo4j_ratings_mismatches += 1
                if src_rating_record["timestamp"] != neo4j_rating_record["timestamp"]:
                    log.error(
                        "Mismatch: Timestamp value differs in Neo4j",
                        user_id=user_id,
                        movie_id=movie_id,
                    )
                    self.neo4j_ratings_mismatches += 1

        self.neo4j_loader.close()
        self.neo4j_ratings_loader.close()
        log.info("--- Data Integrity Audit Finished ---")

        if (
            self.mismatches == 0
            and self.aggregation_mismatches == 0
            and self.neo4j_ratings_mismatches == 0
        ):
            log.info("✅ Audit PASSED: All checks are consistent.")
            return True
        else:
            log.error(
                "❌ Audit FAILED",
                core_mismatches=self.mismatches,
                aggregation_mismatches=self.aggregation_mismatches,
                neo4j_ratings_mismatches=self.neo4j_ratings_mismatches,
            )
            return False


if __name__ == "__main__":
    auditor = Auditor()
    success = auditor.run()
    if not success:
        sys.exit(1)
