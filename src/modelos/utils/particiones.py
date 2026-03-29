"""
particiones.py
--------------
Funciones de partición de datos para los modelos de IgnisAI.
Implementa las tres estrategias de muestreo requeridas.

No ejecutar directamente — importar desde los scripts de modelado.

Estrategias disponibles:
    - split_simple:        División 80/10/10 sin ningún criterio adicional
    - split_estratificado: Mantiene proporción incendios/no incendios en cada split
    - split_con_pesos:     Split estratificado + pesos de clase para compensar desbalanceo

Nota: para regresión solo se usa split_simple_regresion (no hay clases desbalanceadas).
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split,StratifiedKFold, TimeSeriesSplit

# Semilla fija para reproducibilidad (requerido por el profesor)
SEED = 42

# Proporciones de partición
TEST_SIZE = 0.10   # 10% test
VAL_SIZE  = 0.10   # 10% validación → equivale a 0.111 del 90% restante


'''


LAS UNIFICADAS FINALMENTE USADAS COMO PRINCIPALES PARA NUESTROS MODELOS


'''

def split_temporal(X, y, date_col='date', test_size=0.2):
    """
    Se realiza un split teniendo en cuenta una repartición de manera cronológica
    """
    if date_col in X.columns:
        X = X.sort_values(date_col)
        y = y.loc[X.index]
        X = X.drop(columns=[date_col])
        print(f"Dataset ordenado temporalmente y columna '{date_col}' eliminada.")
    else:
        print(f"No se encontró la columna date.")

    split_idx = int(len(X) * (1 - test_size))
    
    X_train = X.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]
    
    return X_train, X_test, y_train, y_test


def generador_cv(tipo_cv='estratificado', n_splits=4, seed=42):
    
    """
    Nos devuelve las diferentes particiones
    """

    if tipo_cv == 'temporal':
        print(f"Usando TimeSeriesSplit con {n_splits} splits.")
        return TimeSeriesSplit(n_splits=n_splits)
    else:
        print(f"Usando StratifiedKFold con {n_splits} splits.")
        return StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=seed)


def get_pesos_clase(y_train):
    """
    Calcula los pesos de clase inversamente proporcionales a su frecuencia.

    Con ratio 25.6x, los incendios reciben peso ~25x mayor que los no incendios.
    Esto compensa el desbalanceo durante el entrenamiento sin modificar los datos.

    Se pasa directamente al modelo con class_weight=get_pesos_clase(y_train)
    o sample_weight=calcular_sample_weights(y_train).

    Returns:
        dict {0: peso_no_incendio, 1: peso_incendio}
    """
    clases, conteos = np.unique(y_train, return_counts=True)
    total = len(y_train)
    n_clases = len(clases)
    pesos = {int(c): total / (n_clases * cnt) for c, cnt in zip(clases, conteos)}

    print(f"  Pesos de clase calculados:")
    print(f"    No incendio (0): {pesos[0]:.4f}")
    print(f"    Incendio    (1): {pesos[1]:.4f}")
    print(f"    Ratio peso  1/0: {pesos[1]/pesos[0]:.1f}x")

    return pesos


def calcular_sample_weights(y_train):
    """
    Genera un array de pesos por muestra para usar en sample_weight.
    Útil para modelos que no aceptan class_weight directamente.

    Returns:
        np.array con el peso correspondiente a cada observación
    """
    pesos = get_pesos_clase(y_train)
    return np.array([pesos[int(yi)] for yi in y_train])


# ── Utilidades internas ────────────────────────────────────────────────────────

def _imprimir_resumen(nombre, y_train, y_val, y_test):
    """Imprime un resumen de la partición para verificación rápida."""
    total = len(y_train) + len(y_val) + len(y_test)
    print(f"\n── Partición {nombre} ──────────────────────────────")
    for nombre_split, y_split in [("Train", y_train), ("Val  ", y_val), ("Test ", y_test)]:
        n = len(y_split)
        inc = y_split.sum()
        print(f"  {nombre_split}: {n:>6,} filas ({n/total*100:.0f}%)  "
              f"| incendios: {inc:>4} ({inc/n*100:.1f}%)")


def _imprimir_resumen_regresion(y_train, y_val, y_test):
    """Imprime un resumen de la partición de regresión."""
    total = len(y_train) + len(y_val) + len(y_test)
    print(f"\n── Partición REGRESIÓN ─────────────────────────────")
    for nombre, y_split in [("Train", y_train), ("Val  ", y_val), ("Test ", y_test)]:
        n = len(y_split)
        print(f"  {nombre}: {n:>5,} filas ({n/total*100:.0f}%)  "
              f"| log_frp media: {y_split.mean():.3f}")