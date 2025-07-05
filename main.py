import structlog
from src.logging_config import setup_logging
from scripts.neo4j_init import initialize_neo4j

from src.extractors.mysql_extractor import MySQLExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.neo4j_loader import Neo4jLoader
from src.conductor import PipelineConductor

setup_logging()
initialize_neo4j()
log = structlog.get_logger()


def main():
    log.info("--- Chariot Data Pipeline: Starting Full Run ---")

    mysql_extractor = MySQLExtractor()
    postgres_loader = PostgresLoader()
    neo4j_loader = Neo4jLoader()

    conductor = PipelineConductor(
        extractor=mysql_extractor, loaders=[postgres_loader, neo4j_loader]
    )

    conductor.run_concurrently()

    neo4j_loader.close()

    log.info("--- Chariot Data Pipeline: Run Finished ---")


if __name__ == "__main__":
    main()
