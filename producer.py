import os
import json
from confluent_kafka import Producer
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
from load_clean_data import load_data, clean_data, oversample
import warnings
import uuid
from datetime import datetime
from loguru import logger
warnings.simplefilter(action='ignore', category=FutureWarning)

# Datos ficticios para la demostración
USER_ID = 1

# Configuración de loguru
logger.add("logs/producer.log", rotation="1 MB", retention="10 days", level="DEBUG") 

# Configuración de Kafka
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

# Callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Mensaje fallido: {err}")
    else:
        logger.success(f"Mensaje enviado a {msg.topic()}")
        logger.info(f"Transaccion: {msg.value()}")

# Cargar y limpiar los datos
data = load_data()
cleandata, X, Y = clean_data(data)
X_resampled, y_resampled = oversample(X, Y)

# Obtener metadata de la tabla, si no lo encuentra lo crea a partir del dataset
if os.path.exists("basedata_metadata.json"):
    metadata = SingleTableMetadata.load_from_json("basedata_metadata.json")
else:
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(X)
    metadata.save_to_json("basedata_metadata.json")

# Definir el modelo CTGAN
ctgan = CTGANSynthesizer(metadata, 
    enforce_rounding=False,
    epochs=20,
    verbose=True)
ctgan.fit(X)

# Generar datos sintéticos
synthetic_data = ctgan.sample(num_rows=8)

# Enviar datos a Kafka
for _, row in synthetic_data.iterrows():
    try:
        message_dict = row.to_dict()

        # Agregar datos adicionales
        message_dict["usuario_id"] = USER_ID
        message_dict["transaccion_id"] = str(uuid.uuid4())

        message_json = json.dumps(message_dict)
        
        producer.produce(
            'fraud_transactions',
            key=str(message_dict.get("user_id", "unknown")),
            value=message_json,
            callback=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        logger.exception(f"Error enviando mensaje: {e}")

# Forzar el envío de todos los mensajes pendientes
producer.flush()
logger.info("Todos los mensajes han sido enviados.")