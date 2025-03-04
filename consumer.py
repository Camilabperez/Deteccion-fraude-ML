from fastapi import FastAPI, HTTPException
from confluent_kafka import Consumer
import mlflow
import pandas as pd
import json
import psycopg2

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

#Consume mensaje
# Consumir mensaje de Kafka
def consume_msj():
    try:
        # Leer mensajes del tópico
        msg = consumer.poll(timeout=5.0)

        # Si no hay mensajes, retorna None
        if msg is None:
            print("consume_msj: No hay mensajes disponibles en Kafka.")
            return None

        # Manejar errores en el mensaje
        if msg.error():
            print(f"consume_msj: Error en Kafka: {msg.error()}")
            return None

        # Procesar el mensaje recibido
        print(f"consume_msj: Mensaje recibido: {msg.value().decode('utf-8')} del tópico {msg.topic()} en la partición {msg.partition()}")
        return msg.value().decode('utf-8')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"consume_msj: Error al procesar: {str(e)}")

############# MLFLOW #############
# Cargar el modelo de MLflow 
def loadmodel():
    model = None
    model_path = "mlruns/307746205249028002/446817692fb0442f990b06954057a8c8/artifacts/logistic_regression_model"

    try:
        model = mlflow.sklearn.load_model(model_path)
        print("loadmodel: Modelo cargado correctamente desde MLflow.")
    except Exception as e:
        print(f"loadmodel: Error al cargar el modelo: {e}")
        model = None
    return model

def get_prediction(msg):
    model = loadmodel()
    try:
        # Convertir de JSON a diccionario
        data_dict = json.loads(msg)  
        data_df = pd.DataFrame([data_dict])  # Crear DataFrame

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
        raise HTTPException(status_code=500, detail=f"get_prediction: Error al procesar el mensaje: {str(e)}")

############# POSTGRE SQL #############
def insert_into_bd(result):
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
            FraudIndicator, Category, TransactionAmount, AnomalyScore, Amount, 
            AccountBalance, SuspiciousFlag, Hour, gap
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    valores = (prediction_label, category, transaction_amount, anomaly_score, amount, accountBalance, suspiciousFlag, hour, gap)

    # Ejecutar la consulta
    cur.execute(sql, valores)
    conn.commit()

    print("Registro insertado correctamente.")

    # Cerrar conexión
    cur.close()
    conn.close()


############# API #############
# Inicializar FastAPI
app = FastAPI()

# Ruta principal
@app.get("/")
def read_root():
    return {"message": "API de Inferencia con Kafka y MLflow"}

# Función para consumir datos de Kafka y realizar predicciones
@app.get("/consume")
def consume():
    print(f"consume: Consumiendo mensajes del tópico: {topic}")
    message = consume_msj()
    result = None
    if message is not None:
        print(f"consume: Consulta a MLFlow")
        result = get_prediction(message)
        insert_into_bd(result)

    prediction = result.get("prediction", "N/A")
    return f"message: {prediction}"

# Función para probar la carga de modelo
@app.get("/carga")
def carga():
    print(f"carga: Consumiendo mensajes del tópico: {topic}")
    message = consume_msj()

    print(f"carga: Carga modelo")
    result = loadmodel()

    return f"message: {message}"

# Función para probar la base de datos
@app.get("/bd")
def bd():
    print(f"bd: prueba bd")
    message = insert_into_bd()

    return f"message: {message}"