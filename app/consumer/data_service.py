import pandas as pd
import numpy as np
from loguru import logger
import joblib, os, json

def get_clean_data(mns):
    try:
        # Convertir de JSON a diccionario
        data_dict = json.loads(mns)  
        data = pd.DataFrame([data_dict]) 

        data['fecha'] = pd.to_datetime(data['fecha'])
        data['LastLogin'] = pd.to_datetime(data['LastLogin'])

        data = data.drop(['transaccion_id', 'usuario_id'], axis=1)
        data['Hour'] = data['fecha'].dt.hour

        data['gap'] = (data['fecha'] - data['LastLogin']).dt.days.abs()
        data['DayOfWeek'] = data['fecha'].dt.dayofweek

        # Mes del año (1=enero, 12=diciembre)
        data['Month'] = data['fecha'].dt.month

        # Proporción del monto sobre el saldo
        data['TransactionAmountRelativeToBalance'] = data['TransactionAmount'] / data['AccountBalance']

        # Eliminar infinitos por divisiones por cero
        data['TransactionAmountRelativeToBalance'].replace([np.inf, -np.inf], np.nan, inplace=True)
        data['TransactionAmountRelativeToBalance'].fillna(0, inplace=True)

        data['BalancePostTransaction'] = data['AccountBalance'] - data['TransactionAmount']
        data['IsWeekend'] = data['DayOfWeek'].isin([5, 6]).astype(int)

        data = data.drop(['fecha','LastLogin'],axis=1)

        data = codificar_categoria(data)

        return data
    
    except Exception as e:
            logger.error(f"Error al limpiar datos: {e}")


def codificar_categoria(mensaje_df):
    encoder = joblib.load('utils/category_encoder.pkl')

    try:
        mensaje_df.loc[:, 'Category'] = encoder.transform([mensaje_df['Category'].iloc[0]])

    except ValueError as e:
        logger.error(f"Categoría desconocida detectada: '{mensaje_df['Category'].iloc[0]}' - {str(e)}")

        # Guardamos la transacción completa en el CSV de errores
        path_error = 'utils/transacciones_no_procesadas.csv'
        
        if not os.path.exists(path_error):
            mensaje_df.to_csv(path_error, index=False)
        else:
            mensaje_df.to_csv(path_error, mode='a', header=False, index=False)

        return None  # Devolvemos None o lo que prefieras para indicar error

    return mensaje_df
