"""
Consumer API - Módulo principal del consumidor de Kafka con FastAPI.

Este módulo expone endpoints para iniciar y detener el consumo de mensajes,
verificar el estado de los servicios relacionados y mostrar o limpiar logs.
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from kafka_consumer import KafkaConsumerService
from service.db import check_postgres
from service.model import check_mlflow
from service.api import check_fastapi_health
from service.dashboard import check_grafana
import os
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger
from service.estado_alerta import alert_state
from pydantic import BaseModel

# Inicializar FastAPI
app = FastAPI()

# Configuración de templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Inicializar servicio del consumidor
consumer_service = KafkaConsumerService(
    topic="fraud_transactions",
    kafka_broker=os.getenv("KAFKA_BROKER", "kafka:9092")
)

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger.add("logs/consumer.log",
           rotation="1 MB",
           retention="10 days",
           level="DEBUG")


@app.get("/")
def root():
    """Endpoint principal.

    Returns:
        dict: Mensaje de bienvenida.
    """
    return {"message":
            "Consumer activo. Usá /admin para ver estado de servicios."}


# Ruta para la página de estado
@app.get("/admin", response_class=HTMLResponse)
def admin(request: Request):
    """Renderiza la vista de estado de servicios externos.

    Args:
        request (Request): Objeto de solicitud de FastAPI.
    Returns:
        HTMLResponse: Página HTML con el estado de los servicios.
    """
    db_status = check_postgres()
    mlflow_status = check_mlflow()
    grafana_status = check_grafana()
    fastapi_status = check_fastapi_health()
    kafka_status = consumer_service.check_kafka()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "db_status": db_status,
        "kafka_status": kafka_status,
        "mLflow_status": mlflow_status,
        "grafana_status": grafana_status,
        "fastapi_status": fastapi_status
    })


@app.get("/admin/panel", response_class=HTMLResponse)
def panel(request: Request):
    return templates.TemplateResponse("panel.html", {"request": request})


@app.get("/start")
def start():
    """Inicia el consumidor de Kafka.

    Returns:
        dict: Mensaje indicando si se inició el consumo correctamente o si ya
        estaba activo.
    """
    return consumer_service.start()


@app.get("/stop")
def stop():
    """Detiene el consumidor de Kafka.

    Returns:
        dict: Mensaje indicando si se detuvo correctamente o si ya
        estaba inactivo.
    """
    return consumer_service.stop()


@app.get("/logs", response_class=PlainTextResponse)
def get_logs():
    """Devuelve el contenido del archivo de log del consumidor.

    Returns:
        str: Contenido del log o mensaje de error si el archivo no existe.
    """
    log_path = "logs/consumer.log"
    if not os.path.exists(log_path):
        return "El archivo de log no existe."

    with open(log_path, "r", encoding="utf-8") as f:
        log_content = f.read()
    return log_content


@app.post("/limpiar_logs")
def limpiar_logs():
    """Limpia el archivo de logs del consumidor.

    Returns:
        dict: Mensaje indicando si la operación fue exitosa o si
        ocurrió un error.
    """
    log_path = "logs/consumer.log"
    try:
        open(log_path, "w").close()
        return {"message": "Logs limpiados correctamente"}
    except Exception as e:
        return {"message": f"Error al limpiar logs: {e}"}


class AlertsPayload(BaseModel):
    enabled: bool


@app.get("/alerts/state")
def get_alerts_state():
    return {"enabled": alert_state.is_enabled()}


@app.post("/alerts/enable")
def enable_alerts():
    alert_state.set_enabled(True)
    logger.info("Alertas habilitadas desde API")
    return {"enabled": True}


@app.post("/alerts/disable")
def disable_alerts():
    alert_state.set_enabled(False)
    logger.info("Alertas deshabilitadas desde API")
    return {"enabled": False}
