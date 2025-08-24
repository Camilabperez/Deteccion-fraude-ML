from loguru import logger
import os, json
from fastapi import HTTPException
import psycopg2
from datetime import datetime
import time


def _pg_connect():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "transactions_db"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

def crear_tabla_postgres():
    logger.info("Conectando a PostgreSQL")
    while True:
        try:
            conn = _pg_connect()
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS transacciones (
                    id SERIAL PRIMARY KEY,
                    usuario_id VARCHAR(50),
                    transaccion_id VARCHAR(50),
                    FraudIndicator VARCHAR(50),
                    Category VARCHAR(50),
                    TransactionAmount DECIMAL(10,2),
                    AnomalyScore DECIMAL(10,5),
                    Amount DECIMAL(10,2),
                    AccountBalance DECIMAL(15,2),
                    SuspiciousFlag INT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    timestamp_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS personas (
                    id     BIGINT PRIMARY KEY,
                    nombre TEXT,
                    email  TEXT NOT NULL
                );
            """)

            cur.execute("""
                INSERT INTO personas (id, nombre, email)
                SELECT s.i,
                       'Persona ' || s.i,
                       'persona' || s.i || '@ejemplo.com'
                FROM generate_series(1, 100) AS s(i)
                LEFT JOIN personas p ON p.id = s.i
                WHERE p.id IS NULL;
            """)

            conn.commit()
            cur.close()
            conn.close()
            logger.success("Base de datos iniciada.")
            break

        except Exception as e:
            logger.error(f"Error iniciando la base de datos: {e}")
            try:
                conn.rollback()
                cur.close()
                conn.close()
            except Exception:
                pass
            time.sleep(5)

def save_to_postgres(result, mns_dict):
    """Inserta una transacción procesada en la base de datos PostgreSQL."""
    try:

        usuario_id = mns_dict.get("usuario_id", "N/A")
        transaccion_id = mns_dict.get("transaccion_id", "N/A")
        prediction_label = result.get("prediction", "N/A")
        category = mns_dict.get("Category", "N/A")
        transaction_amount = result.get("TransactionAmount", "N/A")
        anomaly_score = result.get("AnomalyScore", "N/A")
        amount = result.get("Amount", "N/A")
        accountBalance = result.get("AccountBalance", "N/A")
        suspiciousFlag = result.get("SuspiciousFlag", "0")
        timestamp = mns_dict.get("fecha", "N/A")
        timestamp_procesado = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = _pg_connect()
        cur = conn.cursor()

        sql = """
            INSERT INTO transacciones (
                usuario_id, transaccion_id, FraudIndicator, Category, TransactionAmount, AnomalyScore, Amount, 
                AccountBalance, SuspiciousFlag, fecha, timestamp_procesado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        valores = (
            usuario_id,
            transaccion_id,
            prediction_label,
            category,
            float(transaction_amount),
            float(anomaly_score),
            float(amount),
            float(accountBalance),
            int(suspiciousFlag),
            timestamp,
            timestamp_procesado
        )

        cur.execute(sql, valores)
        conn.commit()
        logger.info("Registro insertado en la base de datos correctamente")
        cur.close()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al guardar los datos en la bd: {str(e)}")


def check_postgres():
    """Verifica la conectividad con la base de datos PostgreSQL."""
    try:
        conn = _pg_connect()
        conn.close()
        return "🟢 Conectado"
    except Exception:
        return "🔴 No disponible"
    

def get_person_email(usuario_id: int) -> str | None:
    """Devuelve email de Personas por id, o None si no existe."""
    try:
        conn = _pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT email FROM personas WHERE id = %s;", (usuario_id,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        try:
            conn.close()
        except:
            pass