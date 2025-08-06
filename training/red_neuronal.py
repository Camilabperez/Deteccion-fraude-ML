import pandas as pd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings as wr
wr.filterwarnings(action="ignore")
wr.filterwarnings("ignore", category=UserWarning, module="matplotlib")
wr.filterwarnings("ignore", category=FutureWarning)
from sklearn.model_selection import StratifiedKFold, GridSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder, MinMaxScaler, RobustScaler
from sklearn.metrics import make_scorer, accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve, auc, precision_recall_curve, confusion_matrix, classification_report, fbeta_score
import seaborn as sns
from imblearn.over_sampling import SMOTE, BorderlineSMOTE, ADASYN, SVMSMOTE
from imblearn.combine import SMOTETomek
from imblearn.pipeline import Pipeline 
from xgboost import XGBClassifier
from tensorflow.keras import Input
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, LeakyReLU, Input
from tensorflow.keras.regularizers import l1, l2, l1_l2
from sklearn.base import BaseEstimator, ClassifierMixin
import numpy as np


x_data = pd.read_csv("x_data.csv")
y = pd.read_csv("y_data.csv")
y_data = y.values.ravel()  


class MyKerasClassifier(BaseEstimator, ClassifierMixin):
    def __init__(self, hidden_layers=[(64, 'relu')], dropout_rate=0.0, optimizer='adam',
                 output_activation='sigmoid', epochs=10, batch_size=32, verbose=0,
                 regularizer=None, regularizer_strength=0.01):
        self.hidden_layers = hidden_layers
        self.dropout_rate = dropout_rate
        self.optimizer = optimizer
        self.output_activation = output_activation
        self.epochs = epochs
        self.batch_size = batch_size
        self.verbose = verbose
        self.regularizer = regularizer  
        self.regularizer_strength = regularizer_strength
        self.model_ = None

    def _get_regularizer(self):
        if self.regularizer == 'l1':
            return l1(self.regularizer_strength)
        elif self.regularizer == 'l2':
            return l2(self.regularizer_strength)
        elif self.regularizer == 'l1_l2':
            return l1_l2(self.regularizer_strength)
        else:
            return None

    def build_model(self, input_dim):
        model = Sequential()
        model.add(Input(shape=(input_dim,)))

        reg = self._get_regularizer()

        for units, activation in self.hidden_layers:
            if activation == 'leaky_relu':
                model.add(Dense(units, kernel_regularizer=reg))
                model.add(LeakyReLU(alpha=0.01))
            else:
                model.add(Dense(units, activation=activation, kernel_regularizer=reg))
            model.add(Dropout(self.dropout_rate))

        model.add(Dense(1, activation=self.output_activation))
        model.compile(loss='binary_crossentropy', optimizer=self.optimizer, metrics=['accuracy'])
        return model

    def fit(self, X, y):
        self.model_ = self.build_model(X.shape[1])
        self.model_.fit(X, y, epochs=self.epochs, batch_size=self.batch_size, verbose=self.verbose)
        return self

    def predict(self, X):
        preds = self.model_.predict(X)
        return (preds > 0.5).astype(int).ravel()

    def predict_proba(self, X):
        preds = self.model_.predict(X)
        return np.hstack([(1 - preds), preds])

    def score(self, X, y):
        from sklearn.metrics import accuracy_score
        return accuracy_score(y, self.predict(X))


scoring = {
    'acc': make_scorer(accuracy_score),
    'precision': make_scorer(precision_score),
    'recall': make_scorer(recall_score),
    'f1': make_scorer(f1_score),
    'auc': make_scorer(roc_auc_score)
}

param_grid = {
    'scaler': [StandardScaler(), RobustScaler()],
    'smote': [SMOTE(random_state=42), ADASYN(random_state=42)],
    'clf__hidden_layers': [
        [(64, 'relu'), (32, 'relu')],
        [(64, 'selu'), (32, 'selu')],
        [(64, 'elu'), (32, 'elu')],
    ],
    'clf__dropout_rate': [0.2, 0.3],
    'clf__optimizer': ['adam', 'rmsprop'],
    'clf__regularizer': ['l1', 'l2', 'l1_l2'],
    'clf__regularizer_strength': [0.001, 0.01],
    'clf__output_activation': ['sigmoid'],
    'clf__epochs': [50],
    'clf__batch_size': [32], #64
}

from imblearn.under_sampling import RandomUnderSampler
pipeline = Pipeline([
    ('scaler', StandardScaler()), 
    ('undersample', RandomUnderSampler(sampling_strategy=0.15, random_state=42)),
    ('smote', SMOTE(random_state=42)),
    ('clf', MyKerasClassifier())
])

grid = GridSearchCV(
    estimator = pipeline,
    param_grid = param_grid,
    scoring = scoring,
    refit = 'auc',  
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
    verbose=2,
    n_jobs=-1,
    return_train_score=True
)
# Ejecutar búsqueda
grid_result = grid.fit(x_data, y_data)

# Guardar resultados
results_df = pd.DataFrame(grid_result.cv_results_)
results_df.to_csv("resultados/red_neuronal_grid_search.csv", index=False, sep='|', decimal=',')
results_df.to_csv("resultados/red_neuronal_grid_search_.csv", index=False)

# Mejor resultado
print("Mejor: %.4f usando %s" % (grid_result.best_score_, grid_result.best_params_))

