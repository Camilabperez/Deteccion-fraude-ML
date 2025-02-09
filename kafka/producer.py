import json
from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

# Callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Mensaje fallido: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# Datos de ejemplo
data = {
    "transaction_id": 12345,
    "amount": 500.75,
    "other_feature": 0.85
}

# Convertir el mensaje a JSON
message = json.dumps(data)

# Enviar mensaje
producer.produce('fraud_transactions', key=str(data["transaction_id"]), value=message, callback=delivery_report)
producer.flush()
