from threading import Thread, Lock, Event
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
    Servicio de consumo Kafka para procesar transacciones.
    Reconstruye el consumidor en cada start() si fue cerrado, y maneja
    correctamente el ciclo de vida del hilo.
    """
    def __init__(self, topic=TOPIC_NAME, kafka_broker=KAFKA_SERVER):
        self.topic = topic
        self.kafka_broker = kafka_broker
        self.kafka_conf = {
            "bootstrap.servers": self.kafka_broker,
            "group.id": "fraud_detection_group",
            "auto.offset.reset": "earliest",
        }

        # Estado de ejecución
        self._lock = Lock()
        self._stop_evt = Event()
        self._thread = None

        # Recursos Kafka / modelo
        self._consumer = None
        self._closed = True
        self._running = False
        self.model = None  # se carga on-demand

    # ---------- utilidades de estado ----------
    def is_closed(self) -> bool:
        return self._closed or self._consumer is None

    def _build_consumer(self):
        # Cierra el anterior si quedara algo
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
        # Crea uno nuevo y se suscribe
        self._consumer = Consumer(self.kafka_conf)
        self._consumer.subscribe([self.topic])
        self._closed = False
        logger.info(f"Consumer reconstruido y suscripto a '{self.topic}'")

    def _ensure_model(self):
        if self.model is None:
            self.model = loadmodel()
        return self.model

    # ---------- API pública ----------
    def start(self):
        """Inicia el hilo de consumo;reconstruye si esta cerrado."""
        with self._lock:
            if self._running:
                return {"message": "El consumidor ya está en ejecución"}

            if self.is_closed():
                self._build_consumer()

            self._stop_evt.clear()
            self._running = True
            self._thread = Thread(target=self._loop,
                                  name="kafka-consumer",
                                  daemon=True)
            self._thread.start()
            return {"message": "Consumidor iniciado"}

    def stop(self):
        """Detiene el hilo y cierra el Consumer."""
        with self._lock:
            if not self._running:
                return {"message": "El consumidor no estaba en ejecución"}

            self._running = False
            self._stop_evt.set()

            # Cerramos el consumer para que poll() termine enseguida
            try:
                if self._consumer is not None:
                    self._consumer.close()
            except Exception as e:
                logger.warning(f"Error cerrando consumer: {e}")
            finally:
                self._closed = True
                self._consumer = None

            t = self._thread
            self._thread = None

        if t:
            t.join(timeout=5)
        logger.info("Consumidor detenido.")
        return {"message": "Consumidor detenido"}

    def restart(self):
        """Atajo útil: detiene y vuelve a iniciar."""
        self.stop()
        return self.start()

    def create_topic(self):
        """Crea el tópico si no existe (usa kafka-python AdminClient)."""
        while True:
            try:
                admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
                topic = NewTopic(name=self.topic,
                                 num_partitions=1,
                                 replication_factor=1)
                admin.create_topics([topic])
                logger.info(f"Tópico '{self.topic}' creado.")
                break
            except TopicAlreadyExistsError:
                logger.info(f"Tópico '{self.topic}' existente.")
                break
            except Exception as e:
                logger.error(f"Esperando Kafka... Error: {e}")
                time.sleep(5)

    def check_kafka(self):
        """Verifica si Kafka está accesible y devuelve el estado."""
        try:
            if self._consumer is None:
                tmp = Consumer(self.kafka_conf)
                md = tmp.list_topics(timeout=5)
                tmp.close()
            else:
                md = self._consumer.list_topics(timeout=5)

            return "🟢 Conectado" if md.topics else "🔴 Sin tópicos"
        except Exception as e:
            logger.error(f"Error en check_kafka: {e}")
            return "🔴 No disponible"

    # ---------- loop interno ----------
    def _loop(self):
        logger.info("Esperando mensajes de Kafka...")
        model = self._ensure_model()

        # Bucle principal de consumo
        while self._running and not self._stop_evt.is_set():
            try:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Error en Kafka: {msg.error()}")
                    continue

                raw = msg.value()
                transaction = (
                    raw.decode("utf-8", errors="ignore")
                    if isinstance(raw, (bytes, bytearray))
                    else str(raw)
                )
                logger.info(f"Mensaje recibido: {transaction}")

                processed = validate_transaction(transaction)
                if processed is None:
                    logger.warning("Transacción descartada.")
                    continue

                # Inferencia
                prediction_result = get_prediction(processed, model)
                prediction = prediction_result.get("prediction", "N/A")
                logger.success(f"Predicción obtenida: {prediction}")

                # Acciones post-inferencia
                transaction_dict = json.loads(transaction)

                if prediction == "fraudulento":
                    if not alert_state.is_enabled():
                        logger.warning(
                            "Alerta NO enviada: envío de alertas "
                            "deshabilitado."
                        )
                    else:
                        try:
                            cust_id = int(transaction_dict.get("CustomerID"))
                            email = get_person_email(cust_id)
                            send_alert_email(email, transaction_dict)
                        except Exception as e:
                            logger.error(f"Error mandando correo: {e}")

                save_to_postgres(prediction_result, transaction_dict)

            except Exception as e:
                if not self._running:
                    break  # nos estamos apagando
                logger.error(f"Error procesando mensaje: {e}")
                time.sleep(1)

        logger.info("Loop de consumo finalizado.")
