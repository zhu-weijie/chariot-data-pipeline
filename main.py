import structlog
import subprocess

from src.logging_config import setup_logging
from scripts.neo4j_init import initialize_neo4j

from src.extractors.mysql_extractor import MySQLExtractor
from src.extractors.mysql_ratings_extractor import MySQLRatingsExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.postgres_ratings_loader import PostgresRatingsLoader
from src.loaders.neo4j_loader import Neo4jLoader
from src.loaders.neo4j_ratings_loader import Neo4jRatingsLoader
from src.conductor import PipelineConductor

setup_logging()
initialize_neo4j()
log = structlog.get_logger()


def main():
    log.info("--- Chariot Data Pipeline: Starting Full Run ---")

    log.info("--- Stage 1: Transferring core movie data ---")
    movies_extractor = MySQLExtractor()
    postgres_movies_loader = PostgresLoader()
    neo4j_movies_loader = Neo4jLoader()
    movies_conductor = PipelineConductor(
        extractor=movies_extractor,
        loaders=[postgres_movies_loader, neo4j_movies_loader],
    )
    movies_conductor.run_concurrently()
    neo4j_movies_loader.close()

    log.info("--- Stage 2: Transferring raw ratings data ---")
    ratings_extractor = MySQLRatingsExtractor()
    postgres_ratings_loader = PostgresRatingsLoader()
    neo4j_ratings_loader = Neo4jRatingsLoader()

    ratings_conductor = PipelineConductor(
        extractor=ratings_extractor,
        loaders=[postgres_ratings_loader, neo4j_ratings_loader],
    )
    ratings_conductor.run_concurrently()
    neo4j_ratings_loader.close()

    log.info("--- Stage 3: Launching parallel ratings aggregation subprocess ---")
    result = subprocess.run(
        ["python", "run_aggregation.py"], capture_output=True, text=True
    )
    log.info("Aggregation subprocess stdout", output=result.stdout)
    if result.returncode != 0:
        log.error("Aggregation subprocess FAILED", stderr=result.stderr)
    else:
        log.info("Aggregation subprocess completed successfully.")

    log.info("--- Chariot Data Pipeline: Run Finished ---")


if __name__ == "__main__":
    main()
