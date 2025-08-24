import requests

def check_grafana():
    """Verifica si Grafana está disponible a través de su API REST."""
    try:
        response = requests.get("http://grafana:3000/api/health")
        if response.status_code == 200 and response.json().get("database") == "ok":
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"