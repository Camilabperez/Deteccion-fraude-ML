import pandas as pd
from loguru import logger
import json

EXPECTED_COLUMNS = [
    "TransactionID",
    "Category",
    "TransactionAmount",
    "AnomalyScore",
    "Timestamp",
    "MerchantID",
    "Amount",
    "CustomerID",
    "Name",
    "Age",
    "Address",
    "AccountBalance",
    "LastLogin",
    "SuspiciousFlag"
]


def validate_transaction(transaction_json):
    """
    Valida que la transacción tenga exactamente los atributos esperados.
    Si faltan, devuelve None y registra log de error.
    Si hay extra, los elimina.
    """
    transaction = json.loads(transaction_json)

    # Verificar que estén todas las columnas esperadas
    missing = [col for col in EXPECTED_COLUMNS if col not in transaction]
    if missing:
        logger.error(f"Transacción inválida: faltan columnas {missing}")
        return None

    # Eliminar columnas no esperadas
    cleaned = {col: transaction[col] for col in EXPECTED_COLUMNS}

    data = pd.DataFrame([cleaned])
    logger.info("Transacción validada correctamente.")
    return data
