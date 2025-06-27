
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from consumer_service import KafkaConsumerService, check_postgres, check_mlflow, check_grafana, check_fastapi_health
import os

templates = Jinja2Templates(directory="templates")

consumer_service = KafkaConsumerService(
    topic="fraud_transactions",
    kafka_broker=os.getenv("KAFKA_BROKER", "kafka:9092")
)

############# API #############
# Inicializar FastAPI
app = FastAPI()

# Ruta principal
@app.get("/")
def root():
    return {"message": "Consumer activo. Usá /admin para ver estado de servicios."}

# Ruta para la página de estado
@app.get("/admin", response_class=HTMLResponse)
def root(request: Request):
    db_status = check_postgres()
    mlflow_status = check_mlflow()
    grafana_status = check_grafana()
    fastapi_status = check_fastapi_health()
    kafka_status = consumer_service.check_kafka()
    return templates.TemplateResponse("status.html", {
        "request": request,
        "db_status": db_status,
        "kafka_status": kafka_status,
        "mLflow_status": mlflow_status,
        "grafana_status": grafana_status,
        "fastapi_status": fastapi_status
    })

# Función para iniciar el proceso de consumo de mensajes de Kafka y realizar predicciones
@app.get("/start")
def start():
    return consumer_service.start()

@app.get("/stop")
def stop():
    return consumer_service.stop()