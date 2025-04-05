from fastapi import FastAPI, HTTPException
from confluent_kafka import Consumer
import mlflow
import pandas as pd
import json
import psycopg2
import subprocess
import time
from loguru import logger
from threading import Thread

# Bandera para controlar el estado del consumidor
consuming = False
consumer_thread = None

# Configuración de loguru
logger.add("logs/consumer.log", rotation="1 MB", retention="10 days", level="DEBUG") 

############# KAFKA CONSUMER #############
# Configuración de Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_conf)
topic = 'fraud_transactions'
consumer.subscribe([topic])

# Consumir mensaje de Kafka
def consume():
    global consuming
    logger.info("Esperando mensajes de Kafka...")
    while consuming:
        try:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Error en Kafka: {msg.error()}")
                continue

            transaction = msg.value().decode('utf-8')
            logger.info(f"Mensaje recibido: {transaction}")

            prediction_result = get_prediction(transaction)
            save_to_postgres(prediction_result)

            prediction = prediction_result.get("prediction", "N/A")
            logger.success(f"Predicción obtenida: {prediction}")

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            time.sleep(1)

    consumer.close()

############# MLFLOW #############
process = subprocess.Popen(
    ["mlflow", "server", "--host", "127.0.0.1", "--port", "8080", "--backend-store-uri", "sqlite:///C:/Users/camil/OneDrive/Escritorio/Tesis/proyectos/main/mlflowfiles/mlflow.db", "--default-artifact-root", "file:///C:/Users/camil/OneDrive/Escritorio/Tesis/proyectos/main/mlflowfiles/artifacts"], 
    stdout=subprocess.PIPE,  # Captura la salida estándar
    stderr=subprocess.PIPE   # Captura errores
)
# Cargar el modelo de MLflow 
def loadmodel():
    model = None
    model_path = "mlruns/307746205249028002/446817692fb0442f990b06954057a8c8/artifacts/logistic_regression_model"

    try:
        model = mlflow.sklearn.load_model(model_path)
        logger.success("loadmodel: Modelo cargado correctamente desde MLflow.")
    except Exception as e:
        logger.error(f"loadmodel: Error al cargar el modelo: {e}")
        model = None
    return model

# Cargar modelo
model = loadmodel()

# Función para realizar la predicción
def get_prediction(msg):
    try:
        # Convertir de JSON a diccionario
        data_dict = json.loads(msg)  
        data_df = pd.DataFrame([data_dict])  

        # Obtener las columnas que el modelo espera
        expected_features = model.feature_names_in_

        # Filtrar solo las columnas que el modelo conoce
        data_df = data_df[expected_features]

        # Realizar la predicción con el modelo de MLflow
        prediction = model.predict(data_df)
        prediction_label = "fraudulento" if prediction[0] == 1 else "no fraudulento"

        return {
            **data_dict,  # Expande los datos originales
            "prediction": prediction_label  # Agrega la predicción
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el mensaje: {str(e)}")

############# POSTGRE SQL #############
# Funcion para guardar en PostgreSQL
def save_to_postgres(result):
    usuario_id = result.get("usuario_id", "N/A")
    transaccion_id = result.get("transaccion_id", "N/A")
    timestamp_generacion  = result.get("timestamp", "N/A")
    prediction_label = result.get("prediction", "N/A")
    category = result.get("Category", "N/A")
    transaction_amount = result.get("TransactionAmount", "N/A")
    anomaly_score = result.get("AnomalyScore", "N/A")
    amount = result.get("Amount", "N/A")
    accountBalance = result.get("AccountBalance", "N/A")
    suspiciousFlag = result.get("SuspiciousFlag", "N/A")
    hour = result.get("Hour", "N/A")
    gap = result.get("gap", "N/A")

    # Configurar la conexión a PostgreSQL
    conn = psycopg2.connect(
        dbname="transactions_db",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )

    # Crear cursor
    cur = conn.cursor()

    # SQL para insertar datos
    sql = """
        INSERT INTO transacciones (
            usuario_id, transaccion_id, timestamp_generacion, FraudIndicator, Category, TransactionAmount, AnomalyScore, Amount, 
            AccountBalance, SuspiciousFlag, Hour, gap
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    valores = (usuario_id, transaccion_id, timestamp_generacion, prediction_label, category, transaction_amount, anomaly_score, amount, accountBalance, suspiciousFlag, hour, gap)

    # Ejecutar la consulta
    cur.execute(sql, valores)
    conn.commit()
    logger.info("Registro insertado en la base de datos correctamente")

    # Cerrar conexión
    cur.close()
    conn.close()


############# API #############
# Inicializar FastAPI
app = FastAPI()

# Ruta principal
@app.get("/")
def read_root():
    return {"message": "Bienvenido a la API de detección de fraudes"}

# Función para iniciar el proceso de consumo de mensajes de Kafka y realizar predicciones
@app.get("/start")
def start_consumer():
    global consuming, consumer_thread
    if consuming:
        return {"message": "El consumidor ya está en ejecución"}

    consuming = True
    consumer_thread = Thread(target=consume)
    consumer_thread.start()
    return {"message": "Consumidor iniciado"}

@app.get("/stop")
def stop_consumer():
    global consuming
    if not consuming:
        return {"message": "El consumidor no estaba en ejecución"}

    consuming = False
    logger.info("Señal enviada para detener el consumidor")
    return {"message": "Consumidor detenido"}
