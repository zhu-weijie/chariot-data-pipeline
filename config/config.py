from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class MySQLSettings(BaseSettings):
    user: str
    password: str
    host: str
    db: str

    model_config = ConfigDict(env_prefix="MYSQL_")


class PostgresSettings(BaseSettings):
    user: str
    password: str
    host: str
    db: str

    model_config = ConfigDict(env_prefix="POSTGRES_")


class Neo4jSettings(BaseSettings):
    user: str
    password: str
    uri: str

    model_config = ConfigDict(env_prefix="NEO4J_")


class EtlSettings(BaseSettings):
    batch_size: int

    model_config = ConfigDict(env_prefix="ETL_")


class Settings(BaseSettings):
    mysql: MySQLSettings = MySQLSettings()
    postgres: PostgresSettings = PostgresSettings()
    neo4j: Neo4jSettings = Neo4jSettings()
    etl: EtlSettings = EtlSettings()


settings = Settings()
