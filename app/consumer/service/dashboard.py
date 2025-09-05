import requests


def check_grafana():
    """Verifica si Grafana está disponible a través de su API REST."""
    try:
        resp = requests.get("http://grafana:3000/api/health")
        if resp.status_code == 200 and resp.json().get("database") == "ok":
            return "🟢 Conectado"
    except Exception:
        pass
    return "🔴 No disponible"
