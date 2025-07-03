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

class KafkaConsumerService:
    model = None  

    def __init__(self, topic="fraud_transactions", kafka_broker="kafka:9092"):
        print("Conectando a Kafka en:", "kafka:9092")
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
                    return
                
                prediction_result = get_prediction(process_mnsg, self.model)
                save_to_postgres(prediction_result, transaction)

                prediction = prediction_result.get("prediction", "N/A")
                logger.success(f"Predicción obtenida: {prediction}")

            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                time.sleep(1)

        self.consumer.close()
        logger.info("Consumidor detenido.")

    def start(self):
        if self.consuming:
            return {"message": "El consumidor ya está en ejecución"}

        self.consuming = True
        self.thread = Thread(target=self.consume_loop)
        self.thread.start()
        return {"message": "Consumidor iniciado"}

    def stop(self):
        if not self.consuming:
            return {"message": "El consumidor no estaba en ejecución"}

        self.consuming = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        return {"message": "Consumidor detenido"}

    def check_kafka(self):
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
# Cargar el modelo de MLflow 
def loadmodel():
    try:
        mlflow.set_tracking_uri("http://mlflow:8080")  # o localhost si fuera externo
        model_path = "./model/logistic_regression_model" # nombre del modelo registrado
        model = mlflow.sklearn.load_model(model_path)
        logger.success("loadmodel: Modelo cargado correctamente desde MLflow Tracking Server.")
        return model
    except Exception as e:
        logger.error(f"loadmodel: Error al cargar el modelo desde MLflow: {e}")
        return None



# Función para realizar la predicción
def get_prediction(data_df, model):
    try:
        # Obtener las columnas que el modelo espera
        expected_features = model.feature_names_in_

        # Filtrar solo las columnas que el modelo conoce
        data_df = data_df[expected_features]

        # Realizar la predicción con el modelo de MLflow
        prediction = model.predict(data_df)
        prediction_label = "fraudulento" if prediction[0] == 1 else "no fraudulento"

        return {
            **data_df,  # Expande los datos originales
            "prediction": prediction_label  # Agrega la predicción
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener prediccion: {str(e)}")

def check_mlflow():
    try:
        response = requests.get("http://mlflow:5000/")
        if response.status_code == 200:
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"

############# POSTGRE SQL #############
# Funcion para guardar en PostgreSQL
def save_to_postgres(result, mns):
    try:
        mns_dict = json.loads(mns)

        usuario_id = mns_dict.get("usuario_id", "N/A")
        transaccion_id = mns_dict.get("transaccion_id", "N/A")
        prediction_label = result.get("prediction", "N/A")

        category = result.get("Category", ["N/A"])[0]
        transaction_amount = result.get("TransactionAmount", ["N/A"])[0]
        anomaly_score = result.get("AnomalyScore", ["N/A"])[0]
        amount = result.get("Amount", ["N/A"])[0]
        accountBalance = result.get("AccountBalance", ["N/A"])[0]
        suspiciousFlag = result.get("SuspiciousFlag", ["N/A"])[0]
        hour = result.get("Hour", ["N/A"])[0]
        gap = result.get("gap", ["N/A"])[0]

        # Configurar la conexión a PostgreSQL
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "transactions_db"),
            user=os.getenv("POSTGRES_USER", "user"),
            password= os.getenv("POSTGRES_PASSWORD", "password"),
            host= os.getenv("POSTGRES_HOST", "localhost"),
            port= os.getenv("POSTGRES_PORT", "5432"),
        )

        # Crear cursor
        cur = conn.cursor()

        # SQL para insertar datos
        sql = """
            INSERT INTO transacciones (
                usuario_id, transaccion_id, FraudIndicator, Category, TransactionAmount, AnomalyScore, Amount, 
                AccountBalance, SuspiciousFlag, Hour, gap
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            usuario_id,
            transaccion_id,
            prediction_label,
            int(category),
            float(transaction_amount),
            float(anomaly_score),
            float(amount),
            float(accountBalance),
            int(suspiciousFlag),
            int(hour),
            int(gap)
        )


        # Ejecutar la consulta
        cur.execute(sql, valores)
        conn.commit()
        logger.info("Registro insertado en la base de datos correctamente")

        # Cerrar conexión
        cur.close()
        conn.close()
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar los datos en la bd: {str(e)}")


def check_postgres():
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
    try:
        response = requests.get("http://grafana:3000/api/health")
        if response.status_code == 200 and response.json().get("database") == "ok":
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"

def check_fastapi_health(url="http://consumer:8080"):
    try:
        response = requests.get(url)
        return "🟢 Conectado" if response.ok else "🔴 Error"
    except:
        return "🔴 No disponible"

