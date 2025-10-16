import asyncio
import json
import logging
from typing import Dict
from aiokafka import AIOKafkaConsumer
from app.config import settings
from app.utils import chunk_message_to_items, sha256_hexdigest
from app.embedder import EmbedderService

logger = logging.getLogger("embedder")
logger.setLevel(logging.INFO)

class KafkaEmbedWorker:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC,
            loop=loop,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            max_poll_records=500
        )
        self.embedder = EmbedderService()
        self.queue = asyncio.Queue()  # queue of chunk items to batch-embed
        self.batch_size = settings.BATCH_SIZE
        self.batch_timeout = settings.BATCH_TIMEOUT_SECONDS

    async def start(self):
        await self.consumer.start()
        logger.info("Kafka consumer started.")
        # start consumer loop and batch worker
        self.consumer_task = self.loop.create_task(self.consume_loop())
        self.batcher_task = self.loop.create_task(self.batcher_loop())

    async def stop(self):
        logger.info("stopping kafka consumer...")
        await self.consumer.stop()
        self.consumer_task.cancel()
        self.batcher_task.cancel()

    async def consume_loop(self):
        try:
            async for msg in self.consumer:
                try:
                    payload = json.loads(msg.value.decode('utf-8'))
                except Exception as e:
                    logger.exception("Invalid JSON from kafka: %s", e)
                    continue

                # convert parser message to per-chunk items
                items = chunk_message_to_items(payload)  # list of dicts with chunk_id, text, checksum, url...
                for it in items:
                    await self.queue.put(it)

        except asyncio.CancelledError:
            logger.info("consumer_loop cancelled")
        except Exception:
            logger.exception("consumer error")

    async def batcher_loop(self):
        """
        Collect items from queue into batches (size or timeout) and process.
        """
        try:
            while True:
                batch = []
                try:
                    # wait for at least one item
                    first = await asyncio.wait_for(self.queue.get(), timeout=self.batch_timeout)
                    batch.append(first)
                except asyncio.TimeoutError:
                    # no item in timeout, continue loop
                    await asyncio.sleep(0.01)
                    continue

                # drain up to batch_size-1 more items without waiting
                while len(batch) < self.batch_size:
                    try:
                        item = self.queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                # process batch asynchronously
                await self.process_batch(batch)
        except asyncio.CancelledError:
            logger.info("batcher_loop cancelled")
        except Exception:
            logger.exception("batcher loop error")

    async def process_batch(self, items):
        """
        items: list of dicts: {'chunk_id','text','checksum','url',...}
        Steps:
          1. check existing checksums in Pinecone via fetch
          2. filter out items with same checksum (skip re-embed)
          3. embed remaining texts (call openai)
          4. upsert vectors into pinecone with metadata
        """
        if not items:
            return

        # dedupe by chunk_id, keep first
        uniq = {}
        for it in items:
            uniq.setdefault(it['chunk_id'], it)
        items = list(uniq.values())

        ids = [it['chunk_id'] for it in items]

        # 1. fetch existing checksums
        existing = {}
        try:
            existing = await self.embedder.fetch_existing_checksums(ids)
        except Exception:
            logger.exception("failed to fetch existing checksums; proceeding to embed all")
            existing = {}

        to_embed = []
        to_embed_items = []
        for it in items:
            existing_checksum = existing.get(it['chunk_id'])
            if existing_checksum and existing_checksum == it['checksum']:
                # skip embedding — nothing changed
                logger.debug("skipping embed for %s (checksum match)", it['chunk_id'])
                continue
            to_embed.append(it['text'])
            to_embed_items.append(it)

        if not to_embed:
            logger.info("batch: nothing to embed (all up-to-date), count=%d", len(items))
            return

        # 2. call openai embedding (in parallel-friendly awaitable)
        try:
            embeddings = await self.embedder.embed_texts(to_embed)
        except Exception:
            logger.exception("embedding failed for batch; will retry individual items")
            # fallback: attempt individual embedding to isolate failure
            embeddings = []
            for txt in to_embed:
                try:
                    vec = await self.embedder.embed_texts([txt])
                    embeddings.append(vec[0])
                except Exception:
                    logger.exception("embedding failed for single item; skipping")
                    embeddings.append(None)

        # 3. prepare upsert items (skip ones where embedding failed)
        upsert_items = []
        for it, emb in zip(to_embed_items, embeddings):
            if emb is None:
                continue
            metadata = {
                "url": it.get("url"),
                "checksum": it.get("checksum"),
                "pos": it.get("pos"),
                "correlationId": it.get("correlationId"),
                "parsed_at": it.get("parsed_at")
            }
            upsert_items.append((it['chunk_id'], emb, metadata))

        if not upsert_items:
            logger.info("after embedding, nothing to upsert")
            return

        # 4. upsert into Pinecone (in chunks to respect limits)
        try:
            await self.embedder.upsert_embeddings(upsert_items)
            logger.info("upserted %d vectors", len(upsert_items))
        except Exception:
            logger.exception("pinecone upsert failed; items will be retried on next message or dead-letter")
