import os
import json
from confluent_kafka import Producer
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import warnings
import uuid
from loguru import logger
from pathlib import Path
import pandas as pd
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
import time
warnings.simplefilter(action='ignore', category=FutureWarning)

# Callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Mensaje fallido: {err}")
    else:
        logger.success(f"Mensaje enviado a {msg.topic()}")
        logger.info(f"Transaccion: {msg.value()}")

def kafka_esta_disponible(broker_url: str, intentos=3, espera=2):
    for i in range(intentos):
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker_url)
            admin.list_topics() 
            return True
        except KafkaError as e:
            logger.info(f"Kafka no disponible - {broker_url} - Intento {i+1}/{intentos}: {e}")
            time.sleep(espera)
    return False

def generar_transacciones(base_path: Path, num_transacciones: int = 8) -> int:
    logger.add("logs/producer.log", rotation="1 MB", retention="10 days", level="DEBUG") 
    broker = os.getenv("KAFKA_BROKER", "localhost:9092")
    conf = {'bootstrap.servers': broker}
    producer = Producer(**conf)

    if not kafka_esta_disponible(broker):
        logger.error("Kafka no está disponible")
        return 0

    X = pd.read_csv(base_path / 'utils/data.csv')
    metadata_path = base_path / 'utils/data_metadata.json'

    if metadata_path.exists():
        metadata = SingleTableMetadata.load_from_json(metadata_path)
    else:
        metadata = SingleTableMetadata()
        metadata.detect_from_dataframe(X)
        metadata.save_to_json(metadata_path)

    ctgan = CTGANSynthesizer(metadata, enforce_rounding=False, epochs=20, verbose=True)
    ctgan.fit(X)

    synthetic_data = ctgan.sample(num_rows=num_transacciones)
    for _, row in synthetic_data.iterrows():
        message_dict = row.to_dict()
        message_dict["usuario_id"] = str(uuid.uuid4())
        message_dict["transaccion_id"] = str(uuid.uuid4())
        message_json = json.dumps(message_dict)

        try:
            producer.produce(
                topic="fraud_transactions",
                key=str(message_dict.get("usuario_id", "none")),
                value=message_json
            )
            producer.poll(0)
        except Exception as e:
            logger.exception(f"Error enviando mensaje: {e}")

    producer.flush()
    logger.info("Transacciones enviadas.")
    return len(synthetic_data)