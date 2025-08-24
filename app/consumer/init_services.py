
import os
from loguru import logger
from service.db import crear_tabla_postgres
from kafka_consumer import KafkaConsumerService

logger.add("logs/consumer.log", rotation="1 MB", retention="10 days", level="DEBUG")

if __name__ == "__main__":
    logger.info("Iniciando servicios")
    crear_tabla_postgres()

    consumer_service = KafkaConsumerService(
        topic="fraud_transactions",
        kafka_broker=os.getenv("KAFKA_BROKER", "kafka:9092")
    )
    consumer_service.create_topic()
