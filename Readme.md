1. Iniciar docker desktop

2. Ejecutar contenedor:
docker-compose up -d
- checkear con docker ps si kafka inicio

3. Publicar API: 
cd kafka
python -m uvicorn consumer:app --reload

4. Enviar mensajefvvv del producer:
cd kafka
python producer.py

5. Para usar la API, en powershell:  
curl.exe -X 'GET' 'http://127.0.0.1:8000/consume' -H 'accept: application/json'

# Kakfa


# Postgres
Para iniciar desde terminal:
docker exec -it main-postgres-1 bash
psql -U user -d transactions_db

Server: localhost
Port: 5432
POSTGRES_USER: user
POSTGRES_PASSWORD: password
POSTGRES_DB: transactions_db

CREATE TABLE transacciones (
    id SERIAL PRIMARY KEY,
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




# Fraud Detection Dataset

🔒 Dataset Description
The Financial Fraud Detection Dataset contains data related to financial transactions and fraudulent patterns. It is designed for the purpose of training and evaluating machine learning models for fraud detection.

📁 Dataset Structure
The dataset is organized within the "data" folder and consists of several subfolders, each containing CSV files with specific information related to financial transactions, customer profiles, fraudulent patterns, transaction amounts, and merchant information. The dataset structure is as follows:

- 📂 data
  - 📂 Transaction Data
    - transaction_records.csv: Contains transaction records with details such as transaction ID, date, amount, and customer ID.
    - transaction_metadata.csv: Contains additional metadata for each transaction.

  - 📂 Customer Profiles
    - customer_data.csv: Includes customer profiles with information such as name, age, address, and contact details.
    - account_activity.csv: Provides details of customer account activity, including account balance, transaction history, and account status.

  - 📂 Fraudulent Patterns
    - fraud_indicators.csv: Contains indicators of fraudulent patterns and suspicious activities.
    - suspicious_activity.csv: Provides specific details of transactions flagged as suspicious.

  - 📂 Transaction Amounts
    - amount_data.csv: Includes transaction amounts for each transaction.
    - anomaly_scores.csv: Provides anomaly scores for transaction amounts, indicating potential fraudulence.

  - 📂 Merchant Information
    - merchant_data.csv: Contains information about merchants involved in transactions.
    - transaction_category_labels.csv: Provides category labels for different transaction types.

📂 src
- data.py: Python file containing code to generate the dataset based on real-world data.

💡 Usage
This dataset can be used for various purposes, including:

- Developing and evaluating machine learning models for financial fraud detection.
- Conducting research on fraud detection algorithms and techniques.
- Training data analysts and data scientists on fraud detection methodologies.

