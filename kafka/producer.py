from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(**conf)

# Callback para manejar la entrega de mensajes
def delivery_report(err, msg):
    if err is not None:
        print(f"Mensaje fallido: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

# Enviar mensaje
producer.produce('fraud_transactions', key="key", value="Hello Kafka!", callback=delivery_report)
producer.flush()
