from fastapi import FastAPI
from pathlib import Path
from producer_service  import generar_transacciones  
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import os

app = FastAPI()

# Habilitar CORS para permitir acceso desde navegador
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o ["http://localhost:8080"] si sabés el origen exacto
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


current_directory = Path.cwd()

@app.get("/")
def root():
    return {"message": "Producer activo. Usá /generar para enviar transacciones."}

@app.post("/generar")
def generar():
    count = generar_transacciones(current_directory)
    return {"message": f"{count} transacciones generadas y enviadas a Kafka"}

@app.get("/logs", response_class=PlainTextResponse)
def get_logs():
    log_path = "logs/producer.log"  # o consumer.log si es el otro servicio
    if not os.path.exists(log_path):
        return "El archivo de log no existe."
    
    with open(log_path, "r", encoding="utf-8") as f:
        log_content = f.read()
    return log_content