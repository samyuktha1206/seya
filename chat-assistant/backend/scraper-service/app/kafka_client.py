import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from .config import settings

consumer: AIOKafkaConsumer | None = None
producer: AIOKafkaProducer | None = None

async def init_kafka():
  global consumer, producer
  consumer = AIOKafkaConsumer(
    settings.topic_in,
    bootstrap_servers=settings.kafka_bootstrap,
    group_id=settings.consumer_group,
    enable_auto_commit=settings.enable_auto_commit,
    max_poll_records=20
  )

  producer = AIOKafkaProducer(
    bootstrap_servers=settings.kafka_bootstrap
  )

  await consumer.start()
  await producer.start()

async def close_kafka():
  global consumer, producer

  if consumer:
    await consumer.stop()

  if producer:
    await producer.stop()