import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "parser.outputs")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "embedder-service-group")

    OPEN_SOURCE_EMBED_MODEL = os.getenv("OPEN_SOURCE_EMBED_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
    PINECONE_ENVIRONMENT = os.getenv("PINECONE_ENVIRONMENT")
    PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")

    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
    BATCH_TIMEOUT_SECONDS = float(os.getenv("BATCH_TIMEOUT_SECONDS", "1.0"))

settings = Settings()
