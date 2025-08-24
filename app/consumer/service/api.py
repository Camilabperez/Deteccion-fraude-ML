import requests

def check_fastapi_health(url="http://consumer:8080"):
    """Verifica si el servicio FastAPI del consumidor está en ejecución."""
    try:
        response = requests.get(url)
        return "🟢 Conectado" if response.ok else "🔴 Error"
    except:
        return "🔴 No disponible"