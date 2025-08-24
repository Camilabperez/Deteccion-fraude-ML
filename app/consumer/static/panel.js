// static/panel.js

// --- Tooltips (no rompas si falta bootstrap) ---
try {
  if (window.bootstrap) {
    const tts = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tts.forEach(el => new bootstrap.Tooltip(el));
  }
} catch (e) { console.warn("Tooltips deshabilitados:", e); }

// ---------- Refs ----------
const consumerBtn         = document.getElementById('execute-consumer-btn');
const consumerBtnStop     = document.getElementById('stop-consumer-btn');
const consumerTerminal    = document.getElementById('consumer-terminal');
const consumerLogOutput   = document.getElementById('consumer-log-output');
const clearConsumerLogs   = document.getElementById('clear-consumer-logs-btn');
const alertaBtn           = document.getElementById('alerta-consumer-btn');

const producerBtn         = document.getElementById('execute-producer-btn');
const producerTerminal    = document.getElementById('producer-terminal');
const producerLogOutput   = document.getElementById('producer-log-output');
const clearProducerLogs   = document.getElementById('clear-producer-logs-btn');

let consumerLogsTimer = null;
let producerLogsTimer = null;
let alertsEnabled = null; // cache local del estado de alertas

// ---------- Helpers ----------
async function fetchLogs(port, logOutput, terminal) {
  try {
    const r = await fetch(`http://localhost:${port}/logs`);
    const text = await r.text();
    logOutput.textContent = text;
    terminal.scrollTop = terminal.scrollHeight;
  } catch (err) {
    logOutput.textContent = "Error cargando logs: " + err.message;
  }
}

// ---- Alertas (toggle con botón) ----
async function getAlertsState() {
  const r = await fetch("/alerts/state");
  const j = await r.json();
  alertsEnabled = !!j.enabled;
  updateAlertButtonUI();
}

async function setAlertsState(enabled) {
  const url = enabled ? "/alerts/enable" : "/alerts/disable";
  const r = await fetch(url, { method: "POST" });
  const j = await r.json();
  alertsEnabled = !!j.enabled;
  updateAlertButtonUI();
}

function updateAlertButtonUI() {
  if (!alertaBtn) return;
  alertaBtn.classList.remove('d-none');
  // Texto/emoji según estado
  if (alertsEnabled === true) {
    alertaBtn.textContent = "🔕 Desactivar alertas por correo";
    alertaBtn.classList.remove("btn-outline-danger");
    alertaBtn.classList.add("btn-outline-secondary");
  } else if (alertsEnabled === false) {
    alertaBtn.textContent = "🔔 Activar alertas por correo";
    alertaBtn.classList.remove("btn-outline-secondary");
    alertaBtn.classList.add("btn-outline-danger");
  } else {
    // estado desconocido mientras carga
    alertaBtn.textContent = "Cargando estado de alertas…";
    alertaBtn.classList.add("disabled");
  }
}

// ---------- Acciones ----------
async function enviarTransacciones() {
  producerTerminal.classList.remove('d-none');
  clearProducerLogs.classList.remove('d-none');
  producerLogOutput.textContent = 'Ejecutando... por favor, espera.';

  const cantidad = document.getElementById("cantidad-input").value;
  const endpoint = `http://localhost:8081/generar/${cantidad}`;
  const response = await fetch(endpoint, { method: 'POST' });

  if (!response.ok) {
    throw new Error(`Error del servidor: ${response.status} ${response.statusText}`);
  }

  await fetchLogs('8081', producerLogOutput, producerTerminal);
  if (producerLogsTimer) clearInterval(producerLogsTimer);
  producerLogsTimer = setInterval(() => fetchLogs('8081', producerLogOutput, producerTerminal), 3000);

  const data = await response.json();
  const slot = document.getElementById("respuesta-transacciones");
  if (slot) slot.textContent = data.message;
}

async function executeAndShowLog(endpoint, port, method, buttonEl, terminalEl, outputEl) {
  terminalEl.classList.remove('d-none');
  clearConsumerLogs.classList.remove('d-none');
  outputEl.textContent = 'Ejecutando... por favor, espera.';
  buttonEl.classList.add('disabled');

  // Mostrar botón de alertas y cargar estado
  if (alertaBtn) {
    alertaBtn.classList.remove('d-none');
    alertaBtn.classList.add('disabled');
    try {
      await getAlertsState();
    } finally {
      alertaBtn.classList.remove('disabled');
    }
  }

  try {
    const response = await fetch(endpoint, { method });
    if (!response.ok) throw new Error(`Error del servidor: ${response.status} ${response.statusText}`);

    await fetchLogs(port, outputEl, terminalEl);
    if (consumerLogsTimer) clearInterval(consumerLogsTimer);
    consumerLogsTimer = setInterval(() => fetchLogs(port, outputEl, terminalEl), 3000);

  } catch (error) {
    outputEl.textContent = `Error al ejecutar la acción:\n${error.message}`;
    consumerBtn.classList.remove('disabled');
  }
}

// ---------- Event listeners ----------
if (consumerBtn) {
  consumerBtn.addEventListener('click', (event) => {
    event.preventDefault();
    consumerBtnStop.classList.remove('disabled');
    executeAndShowLog('http://localhost:8082/start', '8082', 'GET', consumerBtn, consumerTerminal, consumerLogOutput);
  });
}

if (consumerBtnStop) {
  consumerBtnStop.addEventListener('click', (event) => {
    event.preventDefault();
    consumerBtnStop.classList.add('disabled');
    consumerBtn.classList.remove('disabled');
    executeAndShowLog('http://localhost:8082/stop', '8082', 'GET', consumerBtnStop, consumerTerminal, consumerLogOutput);
  });
}

if (clearConsumerLogs) {
  clearConsumerLogs.addEventListener("click", async () => {
    try {
      await fetch("http://localhost:8082/limpiar_logs", { method: 'POST' });
      consumerLogOutput.textContent = "Logs limpiados...";
      setTimeout(() => fetchLogs('8082', consumerLogOutput, consumerTerminal), 500);
    } catch (error) {
      consumerLogOutput.textContent = "Error al limpiar logs: " + error.message;
    }
  });
}

if (clearProducerLogs) {
  clearProducerLogs.addEventListener("click", async () => {
    try {
      await fetch("http://localhost:8081/limpiar_logs", { method: 'POST' });
      producerLogOutput.textContent = "Logs limpiados...";
      setTimeout(() => fetchLogs('8081', producerLogOutput, producerTerminal), 500);
    } catch (error) {
      producerLogOutput.textContent = "Error al limpiar logs: " + error.message;
    }
  });
}

if (alertaBtn) {
  alertaBtn.addEventListener("click", async () => {
    // Si aún no sabemos el estado, lo pedimos primero
    if (alertsEnabled === null) {
      try { await getAlertsState(); } catch {}
    }
    alertaBtn.classList.add("disabled");
    try {
      // Toggle: si está habilitado → deshabilitar; si está deshabilitado → habilitar
      await setAlertsState(!(alertsEnabled === true));
    } catch (e) {
      console.error("No se pudo cambiar el estado de alertas:", e);
    } finally {
      alertaBtn.classList.remove("disabled");
    }
  });
}

// Si querés que al cargar la página ya intente leer el estado (opcional):
document.addEventListener("DOMContentLoaded", () => {
  if (alertaBtn) {
    // No lo mostramos si el backend no responde; executeAndShowLog ya lo hace también
    getAlertsState().catch(() => {});
  }
});
