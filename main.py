import structlog
from src.logging_config import setup_logging
from scripts.neo4j_init import initialize_neo4j

from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.neo4j_loader import Neo4jLoader
from src.conductor import PipelineConductor

from run_aggregation import main as run_ratings_aggregation

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

    log.info("--- Stage 2: Running parallel ratings aggregation pipeline ---")
    run_ratings_aggregation()

    log.info("--- Chariot Data Pipeline: Run Finished ---")


if __name__ == "__main__":
    main()
