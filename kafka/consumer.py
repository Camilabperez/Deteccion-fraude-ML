from fastapi import FastAPI, HTTPException
from confluent_kafka import Consumer
from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table
from sqlalchemy.orm import sessionmaker
import mlflow
import pandas as pd
import psycopg2

# Inicializar FastAPI
app = FastAPI()

# Configuración de Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'fraud_detection_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_conf)
topic = 'fraud_transactions'
consumer.subscribe([topic])

# Configuración de Base de Datos
DATABASE_URL = "postgresql://postgres:admin@localhost:5433/transactions_db"

def select_bd():
    # Crear la conexión
    conn = psycopg2.connect(DATABASE_URL)
    print("Conexión exitosa a la base de datos")

    # Crear un cursor para interactuar con la base de datos
    cursor = conn.cursor()

    # Conexión a la base de datos
    conn = psycopg2.connect(
        dbname="transactions_db",
        user="admin",
        password="admin",
        host="localhost",
        port="5433"
    )
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM transaction.transaction_table;")
    rows = cursor.fetchall()
 

    # Cerrar cursor y conexión
    cursor.close()
    conn.close()

    return rows



# Ruta principal
@app.get("/")
def read_root():
    return {"message": "API de Inferencia con Kafka y MLflow"}

# Función para consumir datos de Kafka y realizar predicciones
@app.get("/consume")
def consume_data():
    print(f"Consumiendo mensajes del tópico: {topic}")
    try:
        while True:
            # Leer mensajes del tópico
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No hay mensajes nuevos, seguir esperando

            if msg.error():
                # Manejar errores
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    return("Fin de la partición")
                else:
                    return(f"Error: {msg.error()}")
                continue

            # Procesar el mensaje recibido
            
            message = f"Recibido: {msg.value().decode('utf-8')} del tópico {msg.topic()} en la partición {msg.partition()}"
            return {"message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar: {str(e)}")
    

    # Todavia no lo termine
@app.get("/consume_selectbd")
def consume_data():
    try:
        msg = consumer.poll(1.0)

        if msg is None:
            raise HTTPException(status_code=404, detail="No hay mensajes disponibles en Kafka.")
        
        if msg.error():
            raise HTTPException(status_code=500, detail=f"Error en Kafka: {msg.error()}")

        # Procesar el mensaje de Kafka
        message_value = msg.value().decode('utf-8')
        print(f"Mensaje recibido: {message_value}")

        # Transformar el mensaje (ejemplo: convertir a un DataFrame)
        data = pd.DataFrame([eval(message_value)])  # Asegúrate de recibir un mensaje compatible con eval()

        # Realizar predicción con el modelo de MLflow
        prediction = model.predict(data)
        prediction_label = "fraudulento" if prediction[0] == 1 else "no fraudulento"

        # Guardar resultado en PostgreSQL
        with engine.connect() as conn:
            insert_stmt = transactions_table.insert().values(
                transaction_id=data['transaction_id'][0],
                amount=data['amount'][0],
                prediction=prediction_label
            )
            conn.execute(insert_stmt)

        
        select_bd()

        return {"transaction_id": data['transaction_id'][0], "prediction": prediction_label}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar: {str(e)}")