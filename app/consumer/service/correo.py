"""
Módulo de envío de correo para alertas de fraude.

Requisitos de entorno (por ejemplo en .env o docker-compose):
    EMAIL_PROVIDER=smtp
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587               # 587 (STARTTLS) recomendado; 465 para SSL
    SMTP_USER=tu_cuenta@gmail.com
    SMTP_PASS=xxxxxxxxxxxxxxxx  # App Password de Gmail (16 chars)
    EMAIL_FROM=tu_cuenta@gmail.com
    EMAIL_FROM_NAME=Alertas Fraude
"""

from __future__ import annotations
import os
import smtplib
import ssl
from email.mime.text import MIMEText
from typing import Any, Dict, Optional
from loguru import logger

provider = os.getenv("EMAIL_PROVIDER", "smtp").lower()
host = os.getenv("SMTP_HOST")
port = int(os.getenv("SMTP_PORT", "587"))
user = os.getenv("SMTP_USER")
password = os.getenv("SMTP_PASS")
from_addr = os.getenv("EMAIL_FROM", "Sistema de deteccion de fraude")
from_name = os.getenv("EMAIL_FROM_NAME", "Alertas Fraude")


def _format_money(value: Optional[float | int | str]) -> str:
    """Formatea montos en formato AR: $ 1.234,56 (tolerante a None/str)."""
    if value in (None, "", "N/A"):
        return "N/D"
    try:
        f = float(value)
    except Exception:
        return str(value)
    txt = f"${f:,.2f}"
    # Cambiar separadores a estilo es_AR
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def _build_html(tx: Dict[str, Any]) -> str:
    """
    Arma el HTML del correo usando los campos típicos.
    Si faltan, se muestran como 'N/D'.
    """
    usuario_id       = tx.get("usuario_id", "N/A") 
    transaccion_id   = tx.get("transaccion_id", "N/D")
    categoria        = tx.get("Category", tx.get("categoria", "N/D"))
    amount           = tx.get("TransactionAmount", tx.get("Amount"))
    amount_txt       = _format_money(amount)
    fecha            = tx.get("fecha", "N/D")

    return f"""
    <html><body>
      <h2>¿Reconoces esta operación?</h2>
      <p> Se detectó una transacción sospechosa asociada a tu cuenta.</p>
      <table border="0" cellspacing="0" cellpadding="6" style="font-family:Arial,Helvetica,sans-serif;">
        <tr><td><b>Usuario</b></td><td>{usuario_id}</td></tr>
        <tr><td><b>ID Transacción</b></td><td>{transaccion_id}</td></tr>
        <tr><td><b>Categoría</b></td><td>{categoria}</td></tr>
        <tr><td><b>Monto</b></td><td>{amount_txt}</td></tr>
        <tr><td><b>Fecha</b></td><td>{fecha}</td></tr>
      </table>
      <p> Si no reconocés esta operación, comunicate de inmediato con soporte.</p>
      <hr/>
      <small> Este es un mensaje automático, por favor no responder.</small>
    </body></html>
    """


def _send_smtp(to_email: str, subject: str, html_body: str) -> None:
    """Envía el correo vía SMTP """

    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_email

    context = ssl.create_default_context()
    with smtplib.SMTP(host, port, timeout=20) as server:
        server.ehlo()
        server.starttls(context=context)
        server.login(user, password)
        server.sendmail(from_addr, [to_email], msg.as_string())


def send_alert_email(to_email: str, tx: Dict[str, Any], subject: Optional[str] = None) -> None:
    """
    Envía un correo de alerta de fraude.
    """
    _subject = subject or f"Alerta de seguridad"

    html = _build_html(tx)

    _send_smtp(to_email, _subject, html)
