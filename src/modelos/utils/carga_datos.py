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

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "extraccion"))
from extraccion.minioFunctions import crear_cliente, bajar_fichero

# ── Configuración ──────────────────────────────────────────────────────────────

PREFIX_GENERAL   = "grupo3/cleaned/modelo_General"
PREFIX_INCENDIOS = "grupo3/cleaned/Modelo_Incendios"
YEARS            = [2022, 2023, 2024, 2025]

# Variables eliminadas correlación alta (r > 0.90)
# - porcentaje:    duplicado casi exacto de grados (r=0.996)
# - temp_max:      casi idéntica a temp_mean (r=0.984)
# - temp_min:      casi idéntica a temp_mean (r=0.977)
# - pressure_mean: casi idéntica a elevacion_centro (r=0.985)
# - NDVI: casi idéntica a NDWI (r=-0.94) --> quitamos NDVI porque tiene menor correlación con la variable objetivo

COLS_ELIMINAR = ["porcentaje", "temp_max", "temp_min", "NDVI", "pressure_mean"]

TARGET_CLASIFICACION = "final"
TARGET_REGRESION     = "log_frp"


# ── Carga desde MinIO ──────────────────────────────────────────────────────────

def _cargar_parquets(prefix: str, years: list) -> pd.DataFrame:
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

def cargar_dataset_general(years=YEARS, eliminar_correladas=True):
    """
    Carga el dataset de clasificación (incendios + no incendios).
    Elimina lat, lon, date y _year — no los incluye en X.

    Returns:
        X (pd.DataFrame): variables predictoras
        y (pd.Series):    variable objetivo (0/1)
    """
    print("Cargando dataset general (clasificación)...")
    df = _cargar_parquets(PREFIX_GENERAL, years)

    if eliminar_correladas:
        df = df.drop(columns=COLS_ELIMINAR, errors="ignore")

    cols_no_features = [TARGET_CLASIFICACION, "lat", "lon", "date", "_year"]
    X = df.drop(columns=[c for c in cols_no_features if c in df.columns])
    y = df[TARGET_CLASIFICACION]

    print(f"  Dataset cargado: {X.shape[0]:,} filas, {X.shape[1]} features")
    print(f"  Incendios:     {y.sum():,} ({y.mean()*100:.1f}%)")
    print(f"  No incendios:  {(y==0).sum():,} ({(y==0).mean()*100:.1f}%)")

    return X, y


def cargar_dataset_general_con_tiempos(years=YEARS, eliminar_correladas=True):
    """
    Igual que cargar_dataset_general pero conserva lat, lon y date en X.
    Necesario para modelos temporales que calculan distancias o ventanas.

    Returns:
        X (pd.DataFrame): variables predictoras + lat, lon, date
        y (pd.Series):    variable objetivo (0/1)
    """
    print("Cargando dataset general con tiempos (clasificación)...")
    df = _cargar_parquets(PREFIX_GENERAL, years)

    if eliminar_correladas:
        df = df.drop(columns=COLS_ELIMINAR, errors="ignore")

    cols_no_features = [TARGET_CLASIFICACION, "_year"]
    X = df.drop(columns=[c for c in cols_no_features if c in df.columns])
    y = df[TARGET_CLASIFICACION]

    print(f"  ✅ Dataset cargado: {X.shape[0]:,} filas, {X.shape[1]} features")
    print(f"  Incendios:     {y.sum():,} ({y.mean()*100:.1f}%)")
    print(f"  No incendios:  {(y==0).sum():,} ({(y==0).mean()*100:.1f}%)")

    return X, y


def cargar_dataset_incendios(years=YEARS, eliminar_correladas=True, logs=True):
    """
    Carga el dataset de regresión (solo incendios, variable objetivo FRP).

    Args:
        eliminar_correladas: si True elimina las 4 variables con r > 0.95
        logs: si True devuelve log(1 + frp_mean) como target (recomendado).
              si False devuelve frp_mean directamente en MW.

    Returns:
        X (pd.DataFrame): variables predictoras
        y (pd.Series):    log(1 + frp_mean) si logs=True, frp_mean si logs=False
    """
    print("Cargando dataset incendios (regresión FRP)...")
    df = _cargar_parquets(PREFIX_INCENDIOS, years)
    df = df.drop(columns=['date_last', 'count', 'lat', 'lon', 'frp_sum', 'final', 'duration_days', 'date'])

    # Eliminar columnas de metadatos que no son features
    
    """cols_metadatos = ["date_last", "count", "lat", "lon", "frp_sum",
                      "final", "duration_days", "date", "_year"]
    df = df.drop(columns=[c for c in cols_metadatos if c in df.columns])
    """
    if eliminar_correladas:
        df = df.drop(columns=COLS_ELIMINAR, errors="ignore")

    frp_col = next(
        (c for c in ["frp_mean", "frp_sum", "frp", "FRP"] if c in df.columns), None
    )
    if frp_col is None:
        raise ValueError(f"No se encontró columna FRP. Columnas: {list(df.columns)}")

    if logs:
        df[TARGET_REGRESION] = np.log1p(df[frp_col])
        X = df.drop(columns=[TARGET_REGRESION, frp_col])
        y = df[TARGET_REGRESION]
    else:
        X = df.drop(columns=[frp_col])
        y = df[frp_col]

    print(f"  ✅ Dataset cargado: {X.shape[0]:,} filas, {X.shape[1]} features")
    print(f"  Target — media: {y.mean():.2f}, std: {y.std():.2f}")
    print(f"  frp_mean — media: {df[frp_col].mean():.1f} MW, max: {df[frp_col].max():.1f} MW")
    if logs:
        print(f"  Nota: para convertir predicciones a MW usa np.expm1(y_pred)")

    return X, y


def cargar_dataset_clasificacion_todas_variables():

    cliente = crear_cliente()
    ruta = 'grupo3/cleaned/final_date_transformado_civilizacion.parquet'
    df = bajar_fichero(cliente, ruta)

    X = df.drop(columns = ['final'])
    y = df['final']

    return X, y