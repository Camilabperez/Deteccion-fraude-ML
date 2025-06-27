FROM python:3.10-slim

WORKDIR /app

# Dependencias del sistema para compilar librerías
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto del código
COPY . .

# Crear carpeta de logs si no existe
RUN mkdir -p logs

# Comando de inicio: correr FastAPI
CMD ["uvicorn", "consumer_app:app", "--host", "0.0.0.0", "--port", "8080"]
