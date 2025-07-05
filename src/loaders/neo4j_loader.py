import structlog
from typing import List, Dict
from neo4j import GraphDatabase

from config.config import settings
from src.interfaces.loader import Loader

log = structlog.get_logger()


class Neo4jLoader(Loader):
    def __init__(self):
        uri = settings.neo4j.uri
        user = settings.neo4j.user
        password = settings.neo4j.password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        log.info("Neo4j Loader initialized.")

    def _get_session(self):
        return self.driver.session()

    def get_high_water_mark(self) -> int:
        query = "MATCH (m:Movie) RETURN MAX(m.movieId) AS max_id"
        log.info("Getting high-water mark from Neo4j.")

        with self._get_session() as session:
            result = session.run(query).single()
            hwm = result["max_id"] if result and result["max_id"] else 0
            log.info("Neo4j high-water mark retrieved", hwm=hwm)
            return hwm

    def _transform_batch(self, batch: List[Dict]) -> List[Dict]:
        for record in batch:
            record["genres"] = record.get("genres", "").strip().split("|")
        return batch

    def write_batch(self, batch: List[Dict]) -> None:
        transformed_batch = self._transform_batch(batch)
        if not transformed_batch:
            log.warn("Batch is empty, nothing to write to Neo4j.")
            return

        query = """
        UNWIND $batch AS movie_data
        MERGE (m:Movie {movieId: movie_data.movieId})
        SET m.title = movie_data.title
        FOREACH (genre_name IN movie_data.genres |
            MERGE (g:Genre {name: genre_name})
            MERGE (m)-[:IN_GENRE]->(g)
        )
        """
        log.info("Writing batch to Neo4j", num_records=len(transformed_batch))

        try:
            with self._get_session() as session:
                session.run(query, batch=transformed_batch)
            log.info("Batch written successfully to Neo4j.")
        except Exception as e:
            log.error("Failed to write batch to Neo4j", error=str(e))
            raise

    def close(self):
        self.driver.close()
