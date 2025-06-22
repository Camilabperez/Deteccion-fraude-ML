# Configuración inicial
Antes de comenzar, es necesario 
1. Tener python, docker, 
1. Crear un entorno virtual para las dependencias,  activarlo e instalar las dependencias del entorno:
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
 ```


# Ejecución del Proyecto
1. Asegurate de tener Docker Desktop corriendo antes de continuar.

2. Levantar los contenedores. Ejecutá el siguiente comando para iniciar los servicios definidos en docker-compose.yml:
```bash
docker-compose up -d
```
Verificá que los contenedores estén activos con:

```bash
docker ps
```
Asegurate de que Kafka y Zookeeper estén en funcionamiento.

3. Iniciar la API con Uvicorn
Desde la raíz del proyecto, ejecutá el siguiente comando:

```bash
python -m uvicorn consumer:app --reload
```

Esto levanta el servidor FastAPI que consume mensajes de Kafka y ejecuta predicciones.

4. Enviar mensajes con el productor
En otra terminal, ejecutá:

```bash
python producer.py
```

Esto simula el envío de transacciones para su análisis.

5. Activar el consumidor desde la API
Podés iniciar el proceso de consumo accediendo al endpoint /start:

```bash
curl.exe -X 'GET' 'http://127.0.0.1:8000/start' -H 'accept: application/json'
```

Esto iniciará el hilo consumidor que escucha el topic de Kafka y evalúa las transacciones entrantes.


# Postgres
La base de datos utilizada para almacenar las transacciones se implementa mediante un contenedor Docker que ejecuta PostgreSQL. A continuación se detallan las configuraciones clave y los comandos para su acceso

##  Acceso desde Terminal
Para acceder al contenedor y utilizar psql para consultas directas, ejecutar:
```bash
docker exec -it postgres bash
psql -U user -d transactions_db
```

**Parametros de conexion:**

- Server: localhost
- Port: 5432
- POSTGRES_USER: user
- POSTGRES_PASSWORD: password
- POSTGRES_DB: transactions_db

**Consultas:**
- Para salir del modo de selección interactiva en psql, presionar la tecla q.

**Esquema de la tabla:**

La base de datos contiene la siguiente tabla principal para registrar transacciones

```sql
CREATE TABLE transacciones (
    id SERIAL PRIMARY KEY,
    usuario_id INT,
    transaccion_id VARCHAR(50),
    FraudIndicator VARCHAR(50), 
    Category INT,      
    TransactionAmount DECIMAL(10,2), 
    AnomalyScore DECIMAL(10,2), 
    Amount DECIMAL(10,2),        
    AccountBalance DECIMAL(15,2), 
    SuspiciousFlag INT,
    Hour INT,                    
    gap DECIMAL(10,2),          
    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);
```
# Visualización con Grafana
Grafana permite conectarse a la base de datos PostgreSQL para visualizar las métricas de las transacciones en tiempo real.

**URL de acceso**: http://localhost:3000/

**Configuración de la conexión a PostgreSQL en Grafana**
- host: postgres:5432
- db:transactions_db
- username: admin / grafanareader
- password: admin
- tsl mode: disabled


# Conjunto de Datos de Detección de Fraude
El conjunto de datos de detección de fraude financiero contiene información relacionada con transacciones financieras y patrones fraudulentos. Está diseñado para el entrenamiento y evaluación de modelos de aprendizaje automático orientados a la detección de fraudes.

## Estructura del Conjunto de Datos
El conjunto de datos se encuentra organizado en la carpeta `data`, que contiene subcarpetas con archivos CSV que incluyen información específica sobre transacciones financieras, perfiles de clientes, patrones fraudulentos, montos de transacciones e información de los comerciantes. La estructura es la siguiente:

- data
  - Transaction Data
    - transaction_records.csv: Contains transaction records with details such as transaction ID, date, amount, and customer ID.
    - transaction_metadata.csv: Contains additional metadata for each transaction.

  - Customer Profiles
    - customer_data.csv: Includes customer profiles with information such as name, age, address, and contact details.
    - account_activity.csv: Provides details of customer account activity, including account balance, transaction history, and account status.

  - Fraudulent Patterns
    - fraud_indicators.csv: Contains indicators of fraudulent patterns and suspicious activities.
    - suspicious_activity.csv: Provides specific details of transactions flagged as suspicious.

  - Transaction Amounts
    - amount_data.csv: Includes transaction amounts for each transaction.
    - anomaly_scores.csv: Provides anomaly scores for transaction amounts, indicating potential fraudulence.

  - Merchant Information
    - merchant_data.csv: Contains information about merchants involved in transactions.
    - transaction_category_labels.csv: Provides category labels for different transaction types.
