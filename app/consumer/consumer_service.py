"""
Kafka Consumer Service - Módulo con la logica del consumo de mensajes desde Kafka, inferencia de fraude con modelo MLflow,
registro en base de datos PostgreSQL y monitoreo de servicios como Grafana, FastAPI y MLflow.
"""
from threading import Thread
from confluent_kafka import Consumer
from data_service import get_clean_data
from loguru import logger
import os, json
import time
from fastapi import HTTPException
import mlflow
import psycopg2
import requests
from datetime import datetime

class KafkaConsumerService:
    """
    Servicio de consumo Kafka para procesar transacciones sospechosas de fraude.
    Lee mensajes del tópico configurado, aplica un modelo de ML y guarda resultados en PostgreSQL.
    """
    model = None  

    def __init__(self, topic="fraud_transactions", kafka_broker="kafka:9092"):
        """Inicializa el consumidor de Kafka, suscribiéndose al tópico y cargando el modelo."""
        self.kafka_conf = {
            'bootstrap.servers': "kafka:9092",
            'group.id': 'fraud_detection_group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.kafka_conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])
        self.consuming = False
        self.thread = None

        self.model = loadmodel()

        logger.add("logs/consumer.log", rotation="1 MB", retention="10 days", level="DEBUG")

    def consume_loop(self):
        """Loop de consumo que recibe, limpia, predice y guarda mensajes mientras el servicio esté activo."""
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

                process_mnsg = get_clean_data(transaction)

                if process_mnsg is None:
                    logger.warning("Transacción descartada.")
                    continue
                
                prediction_result = get_prediction(process_mnsg, self.model)
                prediction = prediction_result.get("prediction", "N/A")
                logger.success(f"Predicción obtenida: {prediction}")

                save_to_postgres(prediction_result, transaction)

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


############# MLFLOW #############
def loadmodel():
    """Carga un modelo registrado desde el MLflow Tracking Server."""
    try:
        mlflow.set_tracking_uri("http://mlflow:8080")  
        model_path = "./model/logistic_regression_model" 
        model = mlflow.sklearn.load_model(model_path)
        logger.success("loadmodel: Modelo cargado correctamente desde MLflow Tracking Server.")
        return model
    except Exception as e:
        logger.error(f"loadmodel: Error al cargar el modelo desde MLflow: {e}")
        return None


def get_prediction(data_df, model, threshold=0.5):
    """Genera una predicción binaria personalizada de fraude usando un umbral (threshold)."""
    try:
        expected_features = model.feature_names_in_
        data_df = data_df[expected_features]

        # Obtener la probabilidad de la clase positiva (fraude = 1)
        proba = model.predict_proba(data_df)[0][1]

        # Aplicar threshold 
        prediction = 1 if proba >= threshold else 0
        prediction_label = "fraudulento" if prediction == 1 else "no fraudulento"

        return {
            **data_df.to_dict(orient="records")[0],  # Convierte a dict plano
            "prediction": prediction_label,
            "probabilidad_fraude": round(proba, 4),
            "umbral_aplicado": threshold
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener predicción: {str(e)}")
    
def check_mlflow():
    """Verifica si el MLflow Tracking Server está disponible."""
    try:
        response = requests.get("http://mlflow:5000/")
        if response.status_code == 200:
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"

############# POSTGRE SQL #############
def save_to_postgres(result, mns):
    """Inserta una transacción procesada en la base de datos PostgreSQL."""
    try:
        mns_dict = json.loads(mns)
        date = datetime.now()

        usuario_id = mns_dict.get("usuario_id", "N/A")
        transaccion_id = mns_dict.get("transaccion_id", "N/A")
        prediction_label = result.get("prediction", "N/A")
        category = mns_dict.get("Category", "N/A")
        transaction_amount = result.get("TransactionAmount", "N/A")
        anomaly_score = result.get("AnomalyScore", "N/A")
        amount = result.get("Amount", "N/A")
        accountBalance = result.get("AccountBalance", "N/A")
        suspiciousFlag = result.get("SuspiciousFlag", "0")
        timestamp = mns_dict.get("fecha", "N/A")
        timestamp_procesado = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "transactions_db"),
            user=os.getenv("POSTGRES_USER", "user"),
            password= os.getenv("POSTGRES_PASSWORD", "password"),
            host= os.getenv("POSTGRES_HOST", "localhost"),
            port= os.getenv("POSTGRES_PORT", "5432"),
        )
        cur = conn.cursor()

        sql = """
            INSERT INTO transacciones (
                usuario_id, transaccion_id, FraudIndicator, Category, TransactionAmount, AnomalyScore, Amount, 
                AccountBalance, SuspiciousFlag, fecha, timestamp_procesado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            usuario_id,
            transaccion_id,
            prediction_label,
            category,
            float(transaction_amount),
            float(anomaly_score),
            float(amount),
            float(accountBalance),
            int(suspiciousFlag),
            timestamp,
            timestamp_procesado
        )

        cur.execute(sql, valores)
        conn.commit()
        logger.info("Registro insertado en la base de datos correctamente")
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar los datos en la bd: {str(e)}")


def check_postgres():
    """Verifica la conectividad con la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "transactions_db"),
            user=os.getenv("POSTGRES_USER", "user"),
            password= os.getenv("POSTGRES_PASSWORD", "password"),
            host= os.getenv("POSTGRES_HOST", "localhost"),
            port= os.getenv("POSTGRES_PORT", "5432"),
        )
        conn.close()
        return "🟢 Conectado"
    except Exception:
        return "🔴 No disponible"
    

def check_grafana():
    """Verifica si Grafana está disponible a través de su API REST."""
    try:
        response = requests.get("http://grafana:3000/api/health")
        if response.status_code == 200 and response.json().get("database") == "ok":
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"


def check_fastapi_health(url="http://consumer:8080"):
    """Verifica si el servicio FastAPI del consumidor está en ejecución."""
    try:
        response = requests.get(url)
        return "🟢 Conectado" if response.ok else "🔴 Error"
    except:
        return "🔴 No disponible"

