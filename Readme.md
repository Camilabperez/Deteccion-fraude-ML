# Sistema de detección de transacciones fraudulentas usando machine learning y microservicios
Este proyecto implementa un pipeline completo para la detección automática de fraudes en transacciones utilizando un modelo de machine learning registrado en MLflow. El sistema opera de forma asíncrona mediante Kafka, y expone una API con FastAPI que escucha eventos, realiza transformaciones y predicciones, y almacena los resultados en PostgreSQL. Para el monitoreo en tiempo real se utiliza Grafana, conectado a la base de datos.

El sistema está completamente contenerizado con Docker y orquestado mediante Docker Compose, permitiendo una puesta en marcha sencilla y replicable.

## Estructura del proyecto
main/
- docker-compose.yml: Orquestación de todos los servicios
- README.md: Documentación del proyecto
- requirements.txt
- .gitignore

- training/: Entrenamiento y registro del modelo ML
  - data/: Contiene los archivos que se utilizaron como dataset para entrenar a los modelos
  - logs/:
  - resultados/:
  - fraud-detecion.ipybn:
  - load_clean_data.py: 

- consumer-app/: API con FastAPI que consume mensajes de Kafka
  - app/
    - consumer.py : Lógica de consumo y predicción
    - templates/
      - status.html: Vista del estado de los servicios
  - Dockerfile: Imagen del consumer
  - requirements.txt

- producer/: Servicio que simula transacciones
   - send_tx.py
   - Dockerfile
   -requirements.txt

- services/:   
  - grafana/
  - kafka/                 
  - mlflow/  
  - postgres/ 
  - zookeper/                  


## Configuración inicial
Antes de comenzar, es necesario 
1. Tener python, docker, 
1. Crear un entorno virtual para las dependencias,  activarlo e instalar las dependencias del entorno:
```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
 ```


## Ejecución del Proyecto
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

## Servicios
### Postgres
La base de datos utilizada para almacenar las transacciones se implementa mediante un contenedor Docker que ejecuta PostgreSQL. A continuación se detallan las configuraciones clave y los comandos para su acceso

**Acceso desde Terminal**
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


