import structlog
from neo4j import GraphDatabase
from config.config import settings

log = structlog.get_logger()


def initialize_neo4j():
    uri = settings.neo4j.uri
    user = settings.neo4j.user
    password = settings.neo4j.password
    driver = None

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            log.info("Applying Neo4j schema constraints...")

            session.run(
                """
                CREATE CONSTRAINT movie_id_unique IF NOT EXISTS
                FOR (m:Movie) REQUIRE m.movieId IS UNIQUE
            """
            )

            session.run(
                """
                CREATE CONSTRAINT genre_name_unique IF NOT EXISTS
                FOR (g:Genre) REQUIRE g.name IS UNIQUE
            """
            )

            session.run(
                """
                CREATE CONSTRAINT user_id_unique IF NOT EXISTS
                FOR (u:User) REQUIRE u.userId IS UNIQUE
            """
            )

            log.info("Neo4j schema constraints applied successfully.")

    except Exception as e:
        log.error("Failed to apply Neo4j constraints", error=str(e))
    finally:
        if driver:
            driver.close()


if __name__ == "__main__":
    initialize_neo4j()
