import asyncio
import logging
from fastapi import FastAPI, APIRouter
from app.kafka_consumer import KafkaEmbedWorker
from app.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("embedder")

app = FastAPI(title="Embedder Service")

# Health endpoint
router = APIRouter()

@router.get("/health")
async def health():
    return {"status":"ok"}

app.include_router(router)

# startup/shutdown lifecycle
worker: KafkaEmbedWorker = None

@app.on_event("startup")
async def startup_event():
    global worker
    loop = asyncio.get_event_loop()
    worker = KafkaEmbedWorker(loop)
    await worker.start()
    logger.info("embedder service started")

@app.on_event("shutdown")
async def shutdown_event():
    global worker
    if worker:
        await worker.stop()
    logger.info("embedder service stopped")
