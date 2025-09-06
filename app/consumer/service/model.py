from loguru import logger
from fastapi import HTTPException
import mlflow
import requests
import os
import cloudpickle as cp
import json
from mlflow.models import infer_signature
import pandas as pd
from mlflow.tracking import MlflowClient
from mlflow.exceptions import RestException

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")


def loadmodel():
    """Carga un modelo registrado desde el MLflow Tracking Server."""
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        model = mlflow.sklearn.load_model("models:/fraud_pipeline_sk_lr/1")
        logger.success("loadmodel: Modelo cargado correctamente desde MLflow")
        return model
    except Exception as e:
        logger.error(f"loadmodel: Error al cargar el modelo desde MLflow: {e}")
        return None


def get_prediction(data_df, model, threshold=0.58):
    """Genera una predicción binaria de fraude aplicando un umbral."""
    try:
        expected_features = model.feature_names_in_
        data_df = data_df[expected_features]

        # Obtener la probabilidad de la clase positiva (fraude = 1)
        proba = model.predict_proba(data_df)[0][1]

        # Aplicar threshold
        pred = 1 if proba >= threshold else 0
        prediction_label = "fraudulento" if pred == 1 else "no fraudulento"

        return {
            **data_df.to_dict(orient="records")[0],
            "prediction": prediction_label,
            "probabilidad_fraude": round(proba, 4),
            "umbral_aplicado": threshold
        }
    except Exception as e:
        raise HTTPException(status_code=500,
                            detail=f"Error al obtener predicción: {str(e)}")


def check_mlflow():
    """Verifica si el MLflow Tracking Server está disponible."""
    try:
        response = requests.get(MLFLOW_TRACKING_URI)
        if response.status_code == 200:
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"


def init_model():
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    try:
        client.get_registered_model("fraud_pipeline_sk_lr")
    except RestException as e:
        with open("./utils/fraud_pipeline.pkl", "rb") as f:
            model = cp.load(f)

        with open("./utils/expected_columns.json", "r") as f:
            expected_cols = json.load(f)

        example = pd.DataFrame([{c: 0 for c in expected_cols}])

        sig = infer_signature(example, None)

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name="fraud_pipeline_sk_lr",
            signature=sig,
            input_example=example
        )
