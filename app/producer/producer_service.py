"""
Generación de datos sintéticos con modelo CTGAN entrenado previamente

Este script utiliza un modelo CTGAN entrenado previamente para generar 
transacciones sintéticas y enviarlas a un tópico Kafka.

# Entrenamiento previo:
El modelo CTGAN se entrena localmente con SDV y luego se serializa usando joblib.
Esto permite excluir la librería SDV del entorno de producción, haciendo que la 
imagen Docker sea mucho más liviana.

Script de entrenamiento: `training/train_ctgan.py`
Ejecutar y mover el modelo a la carpeta `utils`
"""
import os, json, warnings, uuid, random, time
from confluent_kafka import Producer
from loguru import logger
from pathlib import Path
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from datetime import datetime
from faker import Faker
warnings.simplefilter(action='ignore', category=FutureWarning)


def delivery_report(err, msg):
    """
    Callback utilizado por el productor de Kafka para reportar el resultado de la entrega de un mensaje.

    Args:
        err (KafkaError or None): Error si ocurrió alguno al enviar el mensaje.
        msg (Message): Mensaje enviado a Kafka.
    """
    if err is not None:
        logger.error(f"Mensaje fallido: {err}")
    else:
        logger.success(f"Mensaje enviado a {msg.topic()}")
        logger.info(f"Transaccion: {msg.value()}")

def kafka_esta_disponible(broker_url: str, intentos=3, espera=2):
    """
    Verifica si el broker de Kafka está disponible.

    Args:
        broker_url (str): Dirección del broker de Kafka (ej. 'kafka:9092').
        intentos (int): Número de intentos antes de rendirse.
        espera (int): Tiempo de espera entre intentos en segundos.

    Returns:
        bool: True si el broker responde correctamente, False en caso contrario.
    """
    for i in range(intentos):
        try:
            admin = KafkaAdminClient(bootstrap_servers=broker_url)
            admin.list_topics() 
            return True
        except KafkaError as e:
            logger.info(f"Kafka no disponible - {broker_url} - Intento {i+1}/{intentos}: {e}")
            time.sleep(espera)
    return False

def generar_transacciones(logger, base_path: Path, num_transacciones: int = 8) -> int:
    """
    Genera transacciones sintéticas (sin CTGAN) y las envía a un tópico Kafka.
    """
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    conf = {'bootstrap.servers': broker}
    producer = Producer(**conf)

    if not kafka_esta_disponible(broker):
        logger.error("Kafka no está disponible")
        return 0

    fake = Faker("es_AR")
    categorias = ["Other", "Online", "Travel", "Food", "Retail"]
    date = datetime.now()

    transacciones = []
    for _ in range(num_transacciones):
        transaccion = {
            "usuario_id": str(uuid.uuid4()),
            "transaccion_id": str(uuid.uuid4()),
            "fecha": date.strftime("%Y-%m-%d %H:%M:%S"),
            "Category": random.choice(categorias),
            "TransactionAmount": round(random.uniform(10, 100), 2),
            "AnomalyScore": round(random.uniform(0, 1), 5),
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Amount": round(random.uniform(10, 100), 2),
            "AccountBalance": round(random.uniform(1000, 10000), 2),
            "LastLogin": fake.date_between(start_date="-2y", end_date="today").strftime("%Y-%m-%d"),
            "SuspiciousFlag": random.choices([0, 1], weights=[95, 5])[0], 
        }
        transacciones.append(transaccion)

    for t in transacciones:
        message_json = json.dumps(t)
        try:
            producer.produce(
                topic="fraud_transactions",
                key=t["usuario_id"],
                value=message_json
            )
            producer.poll(0)
        except Exception as e:
            logger.exception(f"Error enviando mensaje: {e}")

    producer.flush()
    logger.info("Transacciones enviadas.")
    return len(transacciones)