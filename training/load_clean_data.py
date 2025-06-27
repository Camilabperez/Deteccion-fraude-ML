import pandas as pd
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE

# Funcion para cargar y limpiar los datos:
# data = load_data()
# data, X, Y = clean_data()
# data, X, Y = feature_engineering(data)
# X_resampled, y_resampled = oversample(X,Y)

def load_data():
    account = pd.read_csv("training/data/Customer Profiles/account_activity.csv")
    customer = pd.read_csv("training/data/Customer Profiles/customer_data.csv")
    fraud = pd.read_csv("training/data/Fraudulent Patterns/fraud_indicators.csv")
    suspision = pd.read_csv("training/data/Fraudulent Patterns/suspicious_activity.csv")
    merchant = pd.read_csv("training/data/Merchant Information/merchant_data.csv")
    tran_cat = pd.read_csv("training/data/Merchant Information/transaction_category_labels.csv")
    amount = pd.read_csv("training/data/Transaction Amounts/amount_data.csv")
    anamoly = pd.read_csv("training/data/Transaction Amounts/anomaly_scores.csv")
    tran_data = pd.read_csv("training/data/Transaction Data/transaction_metadata.csv")
    tran_rec = pd.read_csv("training/data/Transaction Data/transaction_records.csv")

    costumer_data = pd.merge(customer, account, on='CustomerID')
    costumer_data = pd.merge(costumer_data, suspision, on='CustomerID')

    transaction_data1 = pd.merge(fraud, tran_cat, on="TransactionID")
    transaction_data2 = pd.merge(amount, anamoly, on="TransactionID")
    transaction_data3 = pd.merge(tran_data, tran_rec, on="TransactionID")
    transaction_data = pd.merge(transaction_data1, transaction_data2,on="TransactionID")
    transaction_data = pd.merge(transaction_data, transaction_data3,on="TransactionID")

    data = pd.merge(transaction_data, costumer_data,on="CustomerID")

    return data

def clean_data(data):
    columns_to_be_dropped = ['TransactionID','MerchantID','CustomerID','Name', 'Age', 'Address']
    data = data.drop(columns_to_be_dropped, axis=1)

    data['Timestamp1'] = pd.to_datetime(data['Timestamp'])

    data['Hour'] = data['Timestamp1'].dt.hour
    data['LastLogin'] = pd.to_datetime(data['LastLogin'])

    X = data.drop(['FraudIndicator', 'Timestamp1'],axis=1)
    Y = data['FraudIndicator']

    return data, X, Y

def feature_engineering(data):
    data1['gap'] = (data1['Timestamp1'] - data1['LastLogin']).dt.days.abs()

    X = data1.drop(['FraudIndicator','Timestamp','Timestamp1','LastLogin'],axis=1)
    Y = data1['FraudIndicator']
    data1 = data1.drop(['Timestamp','Timestamp1','LastLogin'],axis=1)

    label_encoder = LabelEncoder()
    data1['Category'] = label_encoder.fit_transform(data1['Category'])
    X['Category'] = label_encoder.fit_transform(X['Category'])

    return data1, X, Y

def oversample(X,Y):
    smote = SMOTE(random_state=42)
    X_resampled, y_resampled = smote.fit_resample(X, Y)
    return X_resampled, y_resampled

data = load_data()
data, X, Y = clean_data(data)
X.to_csv('data.csv', index=False)