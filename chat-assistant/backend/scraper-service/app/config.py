from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):

  model_config = SettingsConfigDict(env_file = ".env", extra = "ignore", env_prefix = "SEYA_")

  #Kafka
  kafka_bootstrap: str = Field("localhost:9092")
  topic_in: str = Field("search-result.v1")
  topic_out: str = Field("scraper.fetched.v1")
  topic_dlq: str = Field("search-result.dlq.v1")
  consumer_group: str = Field("scraper-service")
  enable_auto_commit: bool = Field(False)

  # R2 / S3 (Cloudflare R2)
  r2_endpoint: str | None = Field(None)
  r2_access_key_id: str | None = Field(None)
  r2_secret_access_key: str | None = Field(None)
  r2_bucket: str | None = Field(None)

  #HTTP Client settings
  user_agent: str = Field("seya-bot/1.0")
  request_timeout_s: int = Field(20)
  max_content_length_mb: int = Field(8)

  #concurrency and politeness
  concurrency: int = Field(8) #global concurrency limit across all domains
  per_domain_concurrency: int = Field(2)
  per_domain_delay_s: float = Field(0.5)

  #Extarction
  use_playwright: bool = Field(False)
  text_chunk_size: int = Field(50_000)

  #postgres
  database_url: str | None = Field(None)
  DB_POOL_MIN_SIZE: int = Field(1)
  DB_POOL_MAX_SIZE: int = Field(3)

  #TTL defaults in Days
  raw_ttl_days: int = Field(30)
  parsed_ttl_days: int = Field(90)
  
  #Security/Ethics
  respect_robots_txt: bool = Field(True)
  
settings = Settings()
