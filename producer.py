import json
from confluent_kafka import Producer
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
from load_clean_data import load_data, clean_data, oversample
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configuración de Kafka
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

# Callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Mensaje fallido: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# Cargar y limpiar datos
data = load_data()
cleandata, X, Y = clean_data(data)
X_resampled, y_resampled = oversample(X, Y)

metadata = SingleTableMetadata()
metadata.detect_from_dataframe(X)

metadata2 = SingleTableMetadata()
metadata2.detect_from_dataframe(cleandata)

print(metadata2)

ctgan = CTGANSynthesizer(metadata, 
    enforce_rounding=False,
    epochs=20,
    verbose=True)
ctgan.fit(X)

# Generar datos sintéticos
synthetic_data = ctgan.sample(num_rows=1)  # Generamos 10 registros sintéticos
print(synthetic_data)

# Enviar datos a Kafka
for _, row in synthetic_data.iterrows():
    try:
        message_dict = row.to_dict()  # Convertir a diccionario
        message_json = json.dumps(message_dict)  # Convertir a JSON
        producer.produce('fraud_transactions', key=str(message_dict.get("Category", "unknown")), 
                         value=message_json, callback=delivery_report)
        producer.poll(0)  # Procesar callbacks de Kafka en cada iteración
    except Exception as e:
        print(f"Error enviando mensaje: {e}")

# Forzar el envío de todos los mensajes pendientes
producer.flush()