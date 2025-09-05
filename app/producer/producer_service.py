import os
import time
import json
import warnings
import uuid
import random
from datetime import datetime
from loguru import logger
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
warnings.simplefilter(action='ignore', category=FutureWarning)


def delivery_report(err, msg):
    """Callback de entrega."""
    if err is not None:
        logger.error(f"Mensaje fallido: {err}")
    else:
        logger.success(f"Mensaje enviado a {msg.topic()}")
        try:
            logger.debug(f"Transacción: {msg.value().decode('utf-8')}")
        except Exception:
            pass


def kafka_esta_disponible(broker_url: str, intentos=3, espera=2) -> bool:
    """Chequea disponibilidad usando AdminClient de confluent_kafka."""
    for i in range(intentos):
        try:
            admin = AdminClient({"bootstrap.servers": broker_url})
            md = admin.list_topics(timeout=5)
            if md.topics is not None:
                return True
        except Exception as e:
            logger.info(f"Kafka no disponible - {broker_url}: {e}")
            time.sleep(espera)
    return False


def _to_bytes(obj) -> bytes:
    """Serializa a bytes utf-8 (dict→JSON, str→utf-8, int→str→utf-8)."""
    if isinstance(obj, (bytes, bytearray)):
        return bytes(obj)
    if isinstance(obj, str):
        return obj.encode("utf-8")
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def generar_transacciones(num_transacciones: int = 8) -> int:
    """
    Genera transacciones sintéticas y las envía a Kafka.
    Devuelve la cantidad enviadas (len).
    """
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "fraud_transactions")

    conf = {"bootstrap.servers": broker}
    producer = Producer(conf)

    if not kafka_esta_disponible(broker):
        logger.error("Kafka no está disponible")
        return 0

    fake = Faker("es_AR")
    categorias = ["Other", "Online", "Travel", "Food", "Retail"]
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_login_date = fake.date_between(start_date="-2y", end_date="today")

    transacciones = []
    for _ in range(num_transacciones):
        t = {
            "CustomerID": random.randint(1, 100),
            "TransactionID": str(uuid.uuid4()),
            "MerchantID": random.randint(1, 100),
            "fecha": now_str,
            "Category": random.choice(categorias),
            "TransactionAmount": round(random.uniform(10, 100), 2),
            "AnomalyScore": round(random.uniform(0, 1), 5),
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Amount": round(random.uniform(10, 100), 2),
            "AccountBalance": round(random.uniform(1000, 10000), 2),
            "LastLogin": last_login_date.strftime("%Y-%m-%d"),
            "SuspiciousFlag": random.choices([0, 1], weights=[95, 5])[0],
            "Name": fake.name(),
            "Address": fake.address().replace("\n", ", "),
            "Age": random.randint(18, 80),
        }
        transacciones.append(t)

    for t in transacciones:
        try:
            producer.produce(
                topic=topic,
                key=_to_bytes(str(t.get("CustomerID", ""))),
                value=_to_bytes(t),
                on_delivery=delivery_report
            )
            # vacía el buffer de eventos internos (llama callbacks)
            producer.poll(0)
        except BufferError as e:
            # si el buffer está lleno, esperá y reintenta
            logger.warning(f"Buffer lleno, esperando: {e}")
            producer.poll(1.0)
            producer.produce(
                topic=topic,
                key=_to_bytes(str(t.get("CustomerID", ""))),
                value=_to_bytes(t),
                on_delivery=delivery_report
            )
        except Exception as e:
            logger.exception(f"Error enviando mensaje: {e}")

    # Esperar a que se envíe todo
    producer.flush(timeout=10.0)
    logger.info("Transacciones enviadas.")
    return len(transacciones)
