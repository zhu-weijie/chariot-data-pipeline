import structlog
from src.logging_config import setup_logging
from scripts.neo4j_init import initialize_neo4j

from src.extractors.mysql_extractor import MySQLExtractor
from src.extractors.mysql_ratings_extractor import MySQLRatingsExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.postgres_ratings_loader import PostgresRatingsLoader
from src.loaders.neo4j_loader import Neo4jLoader
from src.conductor import PipelineConductor

setup_logging()
initialize_neo4j()
log = structlog.get_logger()


def main():
    log.info("--- Chariot Data Pipeline: Starting Movies Transfer ---")

    movies_extractor = MySQLExtractor()
    postgres_movies_loader = PostgresLoader()
    neo4j_movies_loader = Neo4jLoader()
    movies_conductor = PipelineConductor(
        extractor=movies_extractor,
        loaders=[postgres_movies_loader, neo4j_movies_loader],
    )
    movies_conductor.run_concurrently()
    neo4j_movies_loader.close()

    log.info("--- Chariot Data Pipeline: Starting Ratings Transfer ---")

    ratings_extractor = MySQLRatingsExtractor()
    postgres_ratings_loader = PostgresRatingsLoader()

    ratings_conductor = PipelineConductor(
        extractor=ratings_extractor, loaders=[postgres_ratings_loader]
    )
    ratings_conductor.run_concurrently()

    log.info("--- Chariot Data Pipeline: Run Finished ---")


if __name__ == "__main__":
    main()
