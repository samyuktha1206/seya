import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from Config import settings

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, loop:asyncio.AbstractEventLoop):
        self.loop = loop
        self.producer = AIOKafkaProducer = None
        self.consumer = AIOKafkaConsumer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            loop = self.loop, bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
        )
        await self.prodcuer.start()

        self.consumer = AIOKafkaConsumer(
            settings.TOPIC_IN,
            loop = self.loop,
            bootstrap_servers = settings.KAFKA_BOOTSTARP_SERVERS.split(","),
            group_id = settings.KAFKA_CONSUMER_GROUP,
            enable_auto_commit = False,
            auto_offset_reset = "earliest"
        )
        await self.consumer.start()

        logger.info("kafka producer and consumer started")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()
        logger.info("kafka producer and consumer stopped")

    async def send_message(self, topic:str, message:dict, key: str = None, headers: list = None, partition: int = None):
        if not self.producer:
            raise RuntimeError("kafka producer not started")
        try:
            value_bytes = json.dumps(message).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None
            future = await self.producer.send_and_wait(
                topic, value = value_bytes, key = key_bytes, headers = headers, partition = partition
            )
            logger.info(f"Message sent to topic {topic}, partition {future.partition}, offset {future.offset}")
        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise
        
    async def get_messages(self):
            for msg in self.consumer:
                yield msg
