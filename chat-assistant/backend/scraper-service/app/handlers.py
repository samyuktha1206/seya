import asyncio
import json
import logging
import traceback
from typing import Any

from aiokafka import ConsumerRecord

from . import kafka_client
from .config import settings
from .models import SearchResultEvent
from . import fetcher
from .db import upsert_metadata

logger = logging.getLogger("app.handlers")

#small semaphore to limit how many messages we process concurrently
#fetcher internally has a global semaphore to limit its total concurrency
# this is just to avoid flooding the fetcher when kafka is producing messages fast

_message_task_semaphore = asyncio.Semaphore(settings.concurrency*2)

async def _parse_input_message(value_bytes: bytes) -> SearchResultEvent:
    """
    Parse a raw JSON message into a SearchResultEvent. 
    Supports both pydantic v1/v2 style parsing.
    """
    raw = value_bytes.decode("utf-8")
    try:
        #pydantic v2
        ev = SearchResultEvent.model_validate_json(raw)
    except AttributeError:
        #pydantic v1
        ev = SearchResultEvent.parse_raw(raw)
    return ev

async def handle_record(record: ConsumerRecord, session: Any) -> None:
    """
    Handle a single kafka record.
    - Parse event into SearchResultEvent
    - Call fetcher.fetch_and_store with appropriate callbacks
    - On success: commit will be handles by the main loop after task completion
    - On failure: send DLQ message (and later commit offset so we don't retry endlessly)
    """

    #keep per message context for logs
    try:
        ev = await _parse_input_message(record.value)
    except Exception as e:
        logger.exception("Failed to parse incoming record at %s:%s: %s", record.topic, record.partition, e)

        #send raw to DLQ with parse error info
        if kafka_client.producer:
            try:
                dlq_payload = {
                    "reason": "parse_error",
                    "error": str(e),
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "raw": record.value.decode("utf-8", errors="replace")
                }
                await kafka_client.producer.send_and_wait(settings.topic_dlq, json.sumps(dlq_payload).ecnode("utf-8"))
            except Exception:
                logger.exception("failed to send DLQ message for parse error at %s:%s", record.topic, record.partition)
        return
    url = str(ev.link)
    correlationId = ev.correlationId

    logger.info("handling record: url=%s, corr=%s, topic=%s, partition=%s, offset=%s", url, correlationId, record.topic, record.partition, record.offset)

    #Acquire the lightweight semaphore to avoid scheduling too many tasks at once
    async with _message_task_semaphore:
        try:
            #call fetcher.fetch_and_store; it expects...
            #(url, correlationId, producer, db_upsert_cb, r2_upload_file_db, session)

            await fetcher.fetch_and_store(
                url=url,
                correlationId=correlationId,
                producer=kafka_client.producer,
                db_upsert_cb=upsert_metadata,
                r2_upload_file=fetcher.r2_upload_file,
                session=session
            )
            logger.info("successfully processed record: url=%s, corr=%s, topic=%s, partition=%s, offset=%s", url, correlationId, record.topic, record.partition, record.offset)
        except Exception as e:
            #On error send to DLQ with metadata so someone can inspect and retry later
            logger.exception("failed to process record url=%s, corr=%s, topic=%s, partition=%s, offset=%s: %s", url, correlationId, record.topic, record.partition, record.offset, e)
            try:
                payload = {
                    "reason": "processing_error",
                    "url": url,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                    "correlationId": correlationId
                }
                if kafka_client.producer:
                    await kafka_client.producer.send_and_wait(settings.topic_dlq, json.dumps(payload).encode("utf-8"))
            except Exception:
                logger.exception("failed to send DLQ message for processing error url=%s, corr=%s, topic=%s, partition=%s, offset=%s", url, correlationId, record.topic, record.partition, record.offset)

