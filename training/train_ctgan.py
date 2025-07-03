import pandas as pd
from pathlib import Path
from sdv.single_table import CTGANSynthesizer
from sdv.metadata import SingleTableMetadata
import joblib

# Ruta base
base_path = Path(__file__).parent / "utils"

# Crear carpeta si no existe
base_path.mkdir(parents=True, exist_ok=True)

# 1. Cargar datos reales
df = pd.read_csv(base_path / "data.csv")

# 2. Detectar metadata automáticamente
metadata = SingleTableMetadata()
metadata.detect_from_dataframe(df)

# Guardar metadata para referencia futura
metadata.save_to_json(base_path / "data_metadata.json")

# 3. Entrenar modelo CTGAN
model = CTGANSynthesizer(metadata, enforce_rounding=False, epochs=20, verbose=True)
model.fit(df)

# 4. Guardar el modelo entrenado
joblib.dump(model, base_path / "ctgan_model.pkl")

print("Modelo CTGAN entrenado y guardado exitosamente en: utils/ctgan_model.pkl")