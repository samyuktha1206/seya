from pydantic import BaseSettings, Field, AnyHttpUrl, ConfigDict
from typing import Optional

class Settings(BaseSettings):
    #kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field(..., env = "KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_CONSUMER_GROUP: str = Field(..., env = "KAFKA_CONSUMER_GROUP")
    TOPIC_IN: str = Field(..., env = "TOPIC_IN")
    TOPIC_OUT_LLM: str = Field(..., env = "TOPIC_OUT_LLM")
    TOPIC_OUT_EMBEDDER: str = Field(..., env = "TOPIC_OUT_EMBEDDER")
    TOPIC_DLQ: str = Field("dlq", env = "TOPIC_DLQ")

    #Retry/DLQ
    MAX_RETRIES: int = Field(5, env = "MAX_RETRIES")
    RETRY_BACKOFF_SEC: int = Field(2, env = "RETRY_BACKOFF_SEC")

    #Postgres
    POSTGRES_DSN: str = Field(..., env = "POSTGRES_DSN")
    DB_POOL_MIN_SIZE: int = Field(1, env = "DB_POOL_MIN_SIZE")
    DB_POOL_MAX_SIZE: int = Field(10, env = "DB_POOL_MAX_SIZE")

    #R2
    R2_ENDPOINT: AnyHttpUrl = Field(..., env = "R2_ENDPOINT")
    R2_ACCESS_KEY_ID: str = Field(..., env = "R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY: str = Field(..., env =" R2_SECRET_ACCESS_KEY")
    R2_BUCKET: str = Field(..., env = "R2_BUCKET")

    #Parser behaviour
    MAX_INLINE_CHARS: int = Field(4000, env = "MAX_INLINE_CHARS")
    CHUNK_MAX_CHARS: int = Field(2000, env = "CHUNK_MAX_CHARS")
    CHUNK_OVERLAP: int = Field(200, env = "CHUNK_OVERLAP")

    #Opeartional
    MAX_CONCURRENT_PARSERS: int =Field(8, env = "MAX_CONCURRENT_PARSERS")

    model_config = ConfigDict(env_file = ".env", env_file_encoding = "utf-8", extra = "ignore")


settings = Settings()
