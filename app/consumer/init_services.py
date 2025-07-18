import psycopg2
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import os
from loguru import logger

logger.add("logs/consumer.log", rotation="1 MB", retention="10 days", level="DEBUG")

# Parámetros configurables por variables de entorno o valores por defecto
DB_HOST = os.getenv("DB_HOST", "postgresbd")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "transactions_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "fraud_transactions")

# Tabla SQL a crear si no existe
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transacciones (
    id SERIAL PRIMARY KEY,
    usuario_id INT,
    transaccion_id VARCHAR(50),
    FraudIndicator VARCHAR(50), 
    Category INT,      
    TransactionAmount DECIMAL(10,2), 
    AnomalyScore DECIMAL(10,2), 
    Amount DECIMAL(10,2),        
    AccountBalance DECIMAL(15,2), 
    SuspiciousFlag INT,
    Hour INT,                    
    gap DECIMAL(10,2),          
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);
"""

def crear_tabla_postgres():
    logger.info("Conectando a PostgreSQL...")
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD
            )
            cur = conn.cursor()
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Tabla creada o ya existente.")
            break
        except Exception as e:
            logger.error(f"Esperando PostgreSQL... Error: {e}")
            time.sleep(5)

def crear_topico_kafka():
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])
            logger.info(f"Tópico '{TOPIC_NAME}' creado.")
            break
        except TopicAlreadyExistsError:
            logger.info(f"Tópico '{TOPIC_NAME}' existennte.")
            break
        except Exception as e:
            logger.error(f"Esperando Kafka... Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Iniciando servicios")
    crear_tabla_postgres()
    crear_topico_kafka()
