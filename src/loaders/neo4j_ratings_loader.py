import structlog
from typing import List, Dict, Tuple
from neo4j import GraphDatabase
from decimal import Decimal

from config.config import settings
from src.interfaces.loader import Loader

log = structlog.get_logger()


class Neo4jRatingsLoader(Loader):
    def __init__(self):
        uri = settings.neo4j.uri
        user = settings.neo4j.user
        password = settings.neo4j.password
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        log.info("Neo4j Ratings Loader initialized.")

    def _get_session(self):
        return self.driver.session()

    def get_high_water_mark(self) -> Tuple[int, int]:
        query = """
            MATCH (u:User)-[r:RATED]->(m:Movie)
            WITH u.userId AS userId, m.movieId AS movieId
            ORDER BY userId DESC, movieId DESC
            LIMIT 1
            RETURN userId, movieId
        """
        log.info("Getting ratings high-water mark from Neo4j.")

        with self._get_session() as session:
            result = session.run(query).single()
            if result:
                hwm = (result["userId"], result["movieId"])
                log.info("Neo4j ratings high-water mark retrieved", hwm=hwm)
                return hwm
            else:
                log.info("No ratings found in Neo4j, starting from scratch.")
                return (0, 0)

    def _transform_batch(self, batch: List[Dict]) -> List[Dict]:
        for record in batch:
            if "rating" in record and isinstance(record["rating"], Decimal):
                record["rating"] = float(record["rating"])
        return batch

    def write_batch(self, batch: List[Dict]) -> None:
        if not batch:
            log.warn("Batch is empty, nothing to write to Neo4j.")
            return

        transformed_batch = self._transform_batch(batch)

        query = """
        UNWIND $batch AS rating_data
        
        MATCH (m:Movie {movieId: rating_data.movieId})
        MERGE (u:User {userId: rating_data.userId})
        MERGE (u)-[r:RATED]->(m)
        SET r.rating = rating_data.rating, r.timestamp = rating_data.timestamp
        """
        log.info("Writing ratings batch to Neo4j", num_records=len(transformed_batch))

        try:
            with self._get_session() as session:
                session.run(query, batch=transformed_batch)
            log.info("Ratings batch written successfully to Neo4j.")
        except Exception as e:
            log.error("Failed to write ratings batch to Neo4j", error=str(e))
            raise

    def close(self):
        self.driver.close()
