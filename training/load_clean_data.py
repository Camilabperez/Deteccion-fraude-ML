import pandas as pd

# Funcion para cargar y limpiar los datos:
# data = load_data()
# data, X, Y = clean_data()


def load_data():
    account = pd.read_csv(
        "training/data/Customer Profiles/account_activity.csv")
    customer = pd.read_csv(
        "training/data/Customer Profiles/customer_data.csv")
    fraud = pd.read_csv(
        "training/data/Fraudulent Patterns/fraud_indicators.csv")
    suspision = pd.read_csv(
        "training/data/Fraudulent Patterns/suspicious_activity.csv")
    tran_cat = pd.read_csv(
        "training/data/Merchant Information/transaction_category_labels.csv")
    amount = pd.read_csv(
        "training/data/Transaction Amounts/amount_data.csv")
    anamoly = pd.read_csv(
        "training/data/Transaction Amounts/anomaly_scores.csv")
    tran_data = pd.read_csv(
        "training/data/Transaction Data/transaction_metadata.csv")
    tran_rec = pd.read_csv(
        "training/data/Transaction Data/transaction_records.csv")

    costumer_data = pd.merge(customer, account, on='CustomerID')
    costumer_data = pd.merge(costumer_data, suspision, on='CustomerID')

    trans_data1 = pd.merge(fraud, tran_cat, on="TransactionID")
    trans_data2 = pd.merge(amount, anamoly, on="TransactionID")
    trans_data3 = pd.merge(tran_data, tran_rec, on="TransactionID")
    trans_data = pd.merge(trans_data1, trans_data2, on="TransactionID")
    trans_data = pd.merge(trans_data, trans_data3, on="TransactionID")

    data = pd.merge(trans_data, costumer_data, on="CustomerID")

    return data


def clean_data(data):
    col_to_drop = [
        'TransactionID',
        'MerchantID',
        'CustomerID',
        'Name',
        'Age',
        'Address'
    ]
    data = data.drop(col_to_drop, axis=1)

    data['Timestamp1'] = pd.to_datetime(data['Timestamp'])

    data['Hour'] = data['Timestamp1'].dt.hour
    data['LastLogin'] = pd.to_datetime(data['LastLogin'])

    X = data.drop(['FraudIndicator', 'Timestamp1'], axis=1)
    Y = data['FraudIndicator']

    return data, X, Y


data = load_data()
data, X, Y = clean_data(data)
X.to_csv('data.csv', index=False)
