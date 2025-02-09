from confluent_kafka import Consumer, KafkaException

# Configuración del consumidor
conf = {
    'bootstrap.servers': 'localhost:9092',  # Dirección del broker de Kafka
    'group.id': 'fraud_detection_group',    # ID del grupo de consumidores
    'auto.offset.reset': 'earliest'         # Configuración para leer desde el inicio si no hay offsets
}

consumer = Consumer(conf)

# Suscribirse al tópico
topic = 'fraud_transactions'
consumer.subscribe([topic])

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
                print("Fin de la partición")
            else:
                print(f"Error: {msg.error()}")
            continue

        # Procesar el mensaje recibido
        print(f"Recibido: {msg.value().decode('utf-8')} del tópico {msg.topic()} en la partición {msg.partition()}")

except KeyboardInterrupt:
    print("Deteniendo el consumidor...")

finally:
    # Cerrar el consumidor para liberar los recursos
    consumer.close()
