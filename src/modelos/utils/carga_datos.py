"""
carga_datos.py
--------------
Funciones de carga y preprocesamiento básico del dataset IgnisAI.
Usado por todos los scripts de modelado.

No ejecutar directamente — importar desde otros scripts.
"""

import sys
import os
import numpy as np
import pandas as pd

# Añadir extraccion/ al path para usar minioFunctions del proyecto
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "extraccion"))
from minioFunctions import crear_cliente, bajar_fichero

# ── Configuración ──────────────────────────────────────────────────────────────

PREFIX_GENERAL   = "grupo3/cleaned/modelo_General"
PREFIX_INCENDIOS = "grupo3/cleaned/Modelo_Incendios"
YEARS            = [2022, 2023, 2024, 2025]

# Variables eliminadas por multicolinealidad alta (r > 0.95)
# - porcentaje:    duplicado casi exacto de grados (r=0.996)
# - temp_max:      casi idéntica a temp_mean (r=0.984)
# - temp_min:      casi idéntica a temp_mean (r=0.977)
# - pressure_mean: casi idéntica a elevacion_centro (r=0.985)
COLS_ELIMINAR = ["porcentaje", "temp_max", "temp_min", "pressure_mean"]

# Variable objetivo de cada problema
TARGET_CLASIFICACION = "final"
TARGET_REGRESION     = "log_frp"   # transformación log del frp_mean


# ── Carga desde MinIO ──────────────────────────────────────────────────────────

def _cargar_parquets(prefix: str, years: list) -> pd.DataFrame:
    """
    Descarga y concatena los parquets de varios años desde MinIO.
    Uso interno — usar cargar_dataset_general() o cargar_dataset_incendios().
    """
    cliente = crear_cliente()
    dfs = []
    for year in years:
        key = f"{prefix}_{year}.parquet"
        print(f"  Descargando {key}...")
        df = bajar_fichero(cliente, key)
        if df is not None:
            df["_year"] = year
            dfs.append(df)
        else:
            print(f"  ⚠️  No se pudo descargar {key}")

    if not dfs:
        raise RuntimeError(
            "No se cargó ningún archivo. ¿Tienes la VPN de la UCM activa?"
        )
    return pd.concat(dfs, ignore_index=True)


# ── API pública ────────────────────────────────────────────────────────────────

def cargar_dataset_general(years: list = YEARS) -> tuple:
    """
    Carga el dataset de clasificación (incendios + no incendios).

    Aplica:
      - Eliminación de columnas con multicolinealidad alta
      - Separación en X (features) e y (target binario)

    Returns:
        X (pd.DataFrame): variables predictoras
        y (pd.Series):    variable objetivo (0/1)
    """
    print("Cargando dataset general (clasificación)...")
    df = _cargar_parquets(PREFIX_GENERAL, years)

    # Eliminar columnas con multicolinealidad
    df = df.drop(columns=COLS_ELIMINAR, errors="ignore")

    # Separar features y target
    cols_no_features = [TARGET_CLASIFICACION, "lat", "lon", "date", "_year"]
    X = df.drop(columns=[c for c in cols_no_features if c in df.columns])
    y = df[TARGET_CLASIFICACION]

    print(f"  ✅ Dataset cargado: {X.shape[0]:,} filas, {X.shape[1]} features")
    print(f"  Incendios:     {y.sum():,} ({y.mean()*100:.1f}%)")
    print(f"  No incendios:  {(y==0).sum():,} ({(y==0).mean()*100:.1f}%)")

    return X, y


def cargar_dataset_incendios(years: list = YEARS) -> tuple:
    """
    Carga el dataset de regresión (solo incendios, variable objetivo FRP).

    Aplica:
      - Eliminación de columnas con multicolinealidad alta
      - Transformación logarítmica del FRP: log_frp = log(1 + frp_mean)
      - Separación en X (features) e y (log_frp)

    Returns:
        X (pd.DataFrame): variables predictoras
        y (pd.Series):    log(1 + frp_mean) — usar np.expm1(y) para volver a MW
    """
    print("Cargando dataset incendios (regresión FRP)...")
    df = _cargar_parquets(PREFIX_INCENDIOS, years)

    # Eliminar columnas con multicolinealidad
    df = df.drop(columns=COLS_ELIMINAR, errors="ignore")

    # Transformación log del FRP (skewness original = 3.39)
    frp_col = next((c for c in ["frp_mean", "frp_sum", "frp", "FRP"] if c in df.columns), None)
    if frp_col is None:
        raise ValueError(f"No se encontró columna FRP. Columnas: {list(df.columns)}")

    df[TARGET_REGRESION] = np.log1p(df[frp_col])

    # Separar features y target
    cols_no_features = [TARGET_REGRESION, frp_col, "lat", "lon", "date", "_year", "final"]
    X = df.drop(columns=[c for c in cols_no_features if c in df.columns])
    y = df[TARGET_REGRESION]

    print(f"  ✅ Dataset cargado: {X.shape[0]:,} filas, {X.shape[1]} features")
    print(f"  log_frp  — media: {y.mean():.2f}, std: {y.std():.2f}")
    print(f"  frp_mean — media: {df[frp_col].mean():.1f} MW, max: {df[frp_col].max():.1f} MW")
    print(f"  Nota: para convertir predicciones a MW usa np.expm1(y_pred)")

    return X, y