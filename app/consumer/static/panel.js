// static/panel.js

const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
tooltipTriggerList.forEach(tooltipTriggerEl => {
    new bootstrap.Tooltip(tooltipTriggerEl);
});

// CONSUMER
const consumerBtn = document.getElementById('execute-consumer-btn');
const consumerBtnStop = document.getElementById('stop-consumer-btn');
const consumerTerminal = document.getElementById('consumer-terminal');
const consumerLogOutput = document.getElementById('consumer-log-output');
const clearConsumerLogs = document.getElementById("clear-consumer-logs-btn");

// PRODUCER
const producerBtn = document.getElementById('execute-producer-btn');
const producerTerminal = document.getElementById('producer-terminal');
const producerLogOutput = document.getElementById('producer-log-output');
const clearProducerLogs = document.getElementById("clear-producer-logs-btn");

async function fetchLogs(port, logOutput, terminal) {
    try {
        const response = await fetch(`http://localhost:${port}/logs`);
        const text = await response.text();
        logOutput.textContent = text;
        terminal.scrollTop = terminal.scrollHeight;
    } catch (err) {
        logOutput.textContent = "Error cargando logs: " + err.message;
    }
}

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

    fetchLogs('8081', producerLogOutput, producerTerminal);
    setInterval(() => fetchLogs('8081', producerLogOutput, producerTerminal), 3000);

    const data = await response.json();
    document.getElementById("respuesta-transacciones").textContent = data.message;
}

async function executeAndShowLog(endpoint, port, method, buttonEl, terminalEl, outputEl) {
    terminalEl.classList.remove('d-none');
    clearConsumerLogs.classList.remove('d-none');
    outputEl.textContent = 'Ejecutando... por favor, espera.';
    buttonEl.classList.add('disabled');

    try {
        const response = await fetch(endpoint, { method: method });

        if (!response.ok) {
            throw new Error(`Error del servidor: ${response.status} ${response.statusText}`);
        }

        fetchLogs(port, outputEl, terminalEl);
        setInterval(() => fetchLogs(port, outputEl, terminalEl), 3000);

    } catch (error) {
        outputEl.textContent = `Error al ejecutar la acción:\n${error.message}`;
        consumerBtn.classList.remove('disabled');
    }
}

consumerBtn.addEventListener('click', (event) => {
    event.preventDefault();
    consumerBtnStop.classList.remove('disabled');
    executeAndShowLog('http://localhost:8082/start', '8082', 'GET', consumerBtn, consumerTerminal, consumerLogOutput);
});

consumerBtnStop.addEventListener('click', (event) => {
    event.preventDefault();
    consumerBtnStop.classList.add('disabled');
    consumerBtn.classList.remove('disabled');
    executeAndShowLog('http://localhost:8082/stop', '8082', 'GET', consumerBtnStop, consumerTerminal, consumerLogOutput);
});

clearConsumerLogs.addEventListener("click", async () => {
    try {
        const response = await fetch("http://localhost:8082/limpiar_logs", { method: 'POST' });
        consumerLogOutput.textContent = "Logs limpiados...";
        setTimeout(() => {
            fetchLogs('8082', consumerLogOutput, consumerTerminal);
        }, 500);
    } catch (error) {
        consumerLogOutput.textContent = "Error al limpiar logs: " + error.message;
    }
});

clearProducerLogs.addEventListener("click", async () => {
    try {
        const response = await fetch("http://localhost:8081/limpiar_logs", { method: 'POST' });
        producerLogOutput.textContent = "Logs limpiados...";
        setTimeout(() => {
            fetchLogs('8081', producerLogOutput, producerTerminal);
        }, 500);
    } catch (error) {
        producerLogOutput.textContent = "Error al limpiar logs: " + error.message;
    }
});
