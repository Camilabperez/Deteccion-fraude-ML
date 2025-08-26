from loguru import logger
from fastapi import HTTPException
import mlflow
import requests, os

############# MLFLOW #############
def loadmodel():
    """Carga un modelo registrado desde el MLflow Tracking Server."""
    try:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(tracking_uri)
        model_path = "./model/logistic_regression_model"
        model = mlflow.sklearn.load_model(model_path)
        logger.success("loadmodel: Modelo cargado correctamente desde MLflow Tracking Server.")
        return model
    except Exception as e:
        logger.error(f"loadmodel: Error al cargar el modelo desde MLflow: {e}")
        return None


def get_prediction(data_df, model, threshold=0.5):
    """Genera una predicción binaria personalizada de fraude usando un umbral (threshold)."""
    try:
        expected_features = model.feature_names_in_
        data_df = data_df[expected_features]

        # Obtener la probabilidad de la clase positiva (fraude = 1)
        proba = model.predict_proba(data_df)[0][1]

        # Aplicar threshold 
        prediction = 1 if proba >= threshold else 0
        prediction_label = "fraudulento" if prediction == 1 else "no fraudulento"

        return {
            **data_df.to_dict(orient="records")[0], 
            "prediction": prediction_label,
            "probabilidad_fraude": round(proba, 4),
            "umbral_aplicado": threshold
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener predicción: {str(e)}")
    
def check_mlflow():
    """Verifica si el MLflow Tracking Server está disponible."""
    try:
        response = requests.get("http://mlflow:5000/")
        if response.status_code == 200:
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"