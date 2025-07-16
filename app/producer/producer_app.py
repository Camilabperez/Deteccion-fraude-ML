"""
Producer API - Generador de transacciones sintéticas

Este módulo define un servidor FastAPI que actúa como productor de datos para un sistema de detección de fraude.
Permite generar transacciones artificiales usando un modelo de síntesis (CTGAN) y enviarlas a un tópico de Kafka.
"""

from fastapi import FastAPI
from pathlib import Path
from producer_service  import generar_transacciones  
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import os
from fastapi.responses import JSONResponse
from loguru import logger


# Inicializar aplicación FastAPI
app = FastAPI()

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configurar directorio actual y logs
current_directory = Path.cwd()
log_path = current_directory / "logs" / "producer.log"
logger.add(log_path, rotation="1 MB", retention="10 days", level="DEBUG")


@app.get("/")
def root():
    """Ruta principal para verificar si el productor está activo.

    Returns:
        dict: Mensaje de bienvenida.
    """
    return {"message": "Producer activo. Usá /generar para enviar transacciones."}


@app.post("/generar/{cantidad}")
def generar(cantidad: int):
    """Genera y envía transacciones sintéticas a Kafka.

    Args:
        cantidad (int): Número de transacciones a generar.
    Returns:
        JSONResponse: Mensaje con la cantidad generada.
    """
    count = generar_transacciones(logger, current_directory, cantidad)
    logger.info(f"{count} transacciones generadas y enviadas a Kafka")
    return JSONResponse(content={"message": f"{count} transacciones generadas y enviadas a Kafka"})


@app.get("/logs", response_class=PlainTextResponse)
def get_logs():
    """Devuelve el contenido del archivo de logs del productor.

    Returns:
        str: Contenido del archivo de log o mensaje si no existe.
    """
    log_path = "logs/producer.log"  
    if not os.path.exists(log_path):
        return "El archivo de log no existe."
    
    with open(log_path, "r", encoding="utf-8") as f:
        log_content = f.read()
    return log_content


@app.post("/limpiar_logs")
def limpiar_logs():
    """Limpia el contenido del archivo de logs del productor.

    Returns:
        dict: Mensaje indicando si la operación fue exitosa o si ocurrió un error.
    """
    log_path = "logs/producer.log" 
    try:
        open(log_path, "w").close()   
        return {"message": "Logs limpiados correctamente"}
    except Exception as e:
        return {"message": f"Error al limpiar logs: {e}"}