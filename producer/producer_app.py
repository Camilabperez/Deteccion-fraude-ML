from fastapi import FastAPI
from pathlib import Path
from producer_service  import generar_transacciones  

app = FastAPI()
current_directory = Path.cwd()

@app.get("/")
def root():
    return {"message": "Producer activo. Usá /generar para enviar transacciones."}

@app.post("/generar")
def generar():
    count = generar_transacciones(current_directory)
    return {"message": f"{count} transacciones generadas y enviadas a Kafka"}
