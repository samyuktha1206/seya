import asyncio
import logging
import signal
import json
from typing import Optional

import aiohttp
from .config import settings
from . import kafka_client
from . import handlers
from . import db
from .fetcher import global_semaphore
from .playwright_render import set_global_render_semaphore as set_render_sem

logger = logging.getLogger("app.main")
logging.basicConfig(level = logging.INFO, format = "%(asctime)s %(levelname)s %(name)s %(message)s")

async def _consumer_loop(session: aiohttp.ClientSession):
    """
    Main consumer loop that reads msssages from kafka and dispatches them to handlers.
    Commits offsets after processing each record(so DLQ/processing are not rerun).
    """

    consumer = kafka_client.consumer
    if consumer is None:
        raise RuntimeError("Kafka consumer is not initialized")
    
    # use getmany to batch fetches and reduce polling overhead

    try:
        while True:
            # getmany returns dict[TopicPartition, list[ConsumerRecord]]
            records_by_tp = await consumer.getmany(timeout_ms=1000, max_records=50)
            if not records_by_tp:
                await asyncio.sleep(0.01)
                continue
            
            #iterate through topic partitions in deterministic order
            for tp, records in records_by_tp.items():
                for record in records:
                    #spawn a background task but await it immediately so we keep processing serially per-record
                    #you can change to create_task for fully concurrent handling
                    try:
                        await handlers.handle_record(record, session)
                    except Exception:
                        logger.exception("unexpected error handling record at %s:%s:%s", record.topic, record.partition, record.offset)
                    #comit offset after handling this record (manual commit)

                    try:
                        #commit the current position (consumer keeps track of position)
                        await consumer.commit()
                    except Exception:
                        logger.exception("Failed to commit offset after processing record at %s:%s:%s", record.topic, record.partition, record.offset)
    
    except asyncio.CancelledError:
        logger.info("consumer loop cancelled")
        raise
    except Exception:
        logger.exception("consumer loop terminated unexpectedly")
        raise
    
async def _startup():
    # init DB (pool)
    await db.init_db()

    #init kafka
    await kafka_client.init_kafka()

    #optioanl: set playwright render concurrency
    if settings.use_playwright:
        #heuristics: allow 2 concurrent renders by default; tune in settings
        set_render_sem(max(1, min(4, settings.concurrency // 2)))

    #create a shared aiohttp ClientSession (reuse across requests)
    session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=settings.request_timeout_s))
    return session

async def _shutdown(session: aiohttp.ClientSession):
    #Close session

    if session:
        try:
            await session.close()
        except Exception:
            logger.exception("error closing http session")
    #close kafka
    try:
        await kafka_client.close_kafka()
    except Exception:
        logger.exception("error closing kafka")

    #close DB
    try:
        await db.close_db()
    except Exception:
        logger.exception("error closing db")

def _install_signals(loop: asyncio.AbstractEventLoop, cancel_token: asyncio.Event):
    def _handler(sig):
        logger.info("received signal %s, shutting down", sig)
        cancel_token.set()

    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, lambda s=s: _handler(s))

async def main():
    loop = asyncio.get_running_loop()
    cancel_token = asyncio.Event()
    _install_signals(loop, cancel_token)

    session = None
    consumer_task = None
    try:
        session = await _startup()
        consumer_task = asyncio.create_task(_consumer_loop(session))
        #wait until a termination signal is received

        await cancel_token.wait()
        logger.info("shutdown requested, cancelling consumer loop")
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
    finally:
        await _shutdown(session)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass