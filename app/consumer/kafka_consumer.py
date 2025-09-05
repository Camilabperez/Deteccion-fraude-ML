"""
Kafka Consumer Service - Módulo con la logica del consumo de mensajes desde
Kafka, inferencia de fraude con modelo MLflow,
registro en base de datos PostgreSQL y monitoreo de servicios como Grafana,
FastAPI y MLflow.
"""
from threading import Thread
from confluent_kafka import Consumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from service.db import save_to_postgres, get_person_email
from service.model import loadmodel, get_prediction
from service.correo import send_alert_email
from service.process_data import validate_transaction
from service.estado_alerta import alert_state
from loguru import logger
import time
import os
import json

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "fraud_transactions")


class KafkaConsumerService:
    """
    Servicio de consumo Kafka para procesar transacciones sospechosas de
    fraude.Lee mensajes del tópico configurado, aplica un modelo de ML y
    guarda resultados en PostgreSQL.
    """
    model = None

    def __init__(self, topic=TOPIC_NAME, kafka_broker=KAFKA_SERVER):
        """Inicializa el consumidor de Kafka, suscribiéndose al tópico y
        cargando el modelo."""
        self.kafka_conf = {
            'bootstrap.servers': KAFKA_SERVER,
            'group.id': 'fraud_detection_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.kafka_conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])
        self.consuming = False
        self.thread = None

        self.model = loadmodel()

    def create_topic(self):
        logger.info(f"KAFKA_SERVER '{KAFKA_SERVER}' .")
        logger.info(f"TOPIC_NAME '{TOPIC_NAME}' .")
        while True:
            try:
                admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
                topic = NewTopic(name=TOPIC_NAME,
                                 num_partitions=1,
                                 replication_factor=1)
                admin.create_topics([topic])
                logger.info(f"Tópico '{TOPIC_NAME}' creado.")
                break
            except TopicAlreadyExistsError:
                logger.info(f"Tópico '{TOPIC_NAME}' existente.")
                break
            except Exception as e:
                logger.error(f"Esperando Kafka... Error: {e}")
                time.sleep(5)

    def consume_loop(self):
        """Loop de consumo que recibe, limpia, predice y guarda mensajes
        mientras el servicio esté activo."""
        logger.info("Esperando mensajes de Kafka...")
        while self.consuming:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Error en Kafka: {msg.error()}")
                    continue

                transaction = msg.value().decode('utf-8')
                logger.info(f"Mensaje recibido: {transaction}")

                process_mnsg = validate_transaction(transaction)

                if process_mnsg is None:
                    logger.warning("Transacción descartada.")
                    continue
                prediction_result = get_prediction(process_mnsg, self.model)
                prediction = prediction_result.get("prediction", "N/A")
                logger.success(f"Predicción obtenida: {prediction}")

                transaction_dict = json.loads(transaction)
                if (prediction == "fraudulento"):
                    if not alert_state.is_enabled():
                        logger.warning(
                            "Alerta NO enviada: envío de alertas "
                            "está deshabilitado.")
                    else:
                        try:
                            cust_id = int(transaction_dict.get("CustomerID"))
                            email = get_person_email(cust_id)
                            send_alert_email(email, transaction_dict)
                        except Exception as e:
                            logger.error(f"Error mandndo correo : {e}")

                save_to_postgres(prediction_result, transaction_dict)

            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                time.sleep(1)

        self.consumer.close()
        logger.info("Consumidor detenido.")

    def start(self):
        """Inicia el hilo de consumo si no está ya activo."""
        if self.consuming:
            return {"message": "El consumidor ya está en ejecución"}

        self.consuming = True
        self.thread = Thread(target=self.consume_loop)
        self.thread.start()
        return {"message": "Consumidor iniciado"}

    def stop(self):
        """Detiene el hilo de consumo si está activo."""
        if not self.consuming:
            return {"message": "El consumidor no estaba en ejecución"}

        self.consuming = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        return {"message": "Consumidor detenido"}

    def check_kafka(self):
        """Verifica si Kafka está accesible y devuelve el estado."""
        try:
            metadata = self.consumer.list_topics(timeout=5)
            if metadata.topics:
                return "🟢 Conectado"
            else:
                return "🔴 Sin tópicos"
        except Exception as e:
            logger.error(f"Error en check_kafka: {e}")
            return "🔴 No disponible"
