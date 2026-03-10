"""
metricas.py
-----------
Funciones de evaluación para los modelos de IgnisAI.
Centraliza las métricas para no repetir código en cada script de modelado.

No ejecutar directamente — importar desde los scripts de modelado.

Funciones disponibles:
    - evaluar_clasificacion: F1, precision, recall, ROC-AUC, accuracy
    - evaluar_regresion:     RMSE, MAE, R²
"""

import numpy as np
from sklearn.metrics import (
    f1_score, precision_score, recall_score,
    roc_auc_score, accuracy_score, classification_report,
    mean_squared_error, mean_absolute_error, r2_score
)


# ── Clasificación ──────────────────────────────────────────────────────────────

def evaluar_clasificacion(y_true, y_pred, y_prob=None, nombre="Modelo"):
    """
    Calcula y muestra las métricas de clasificación binaria.

    La métrica principal es F1-score porque las clases están muy desbalanceadas
    (ratio 25.6x). Accuracy sola sería engañosa — un modelo que predice siempre
    'no incendio' tendría 96.2% de accuracy siendo completamente inútil.

    Args:
        y_true: valores reales (0/1)
        y_pred: predicciones del modelo (0/1)
        y_prob: probabilidades predichas para clase 1 (opcional, para ROC-AUC)
        nombre: nombre del modelo para mostrar en el resumen

    Returns:
        dict con todas las métricas
    """
    f1        = f1_score(y_true, y_pred, zero_division=0)
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall    = recall_score(y_true, y_pred, zero_division=0)
    accuracy  = accuracy_score(y_true, y_pred)
    roc_auc   = roc_auc_score(y_true, y_prob) if y_prob is not None else None

    print(f"\n── Métricas {nombre} ──────────────────────────────")
    print(f"  F1-score  (principal): {f1:.4f}")
    print(f"  Precision            : {precision:.4f}")
    print(f"  Recall               : {recall:.4f}")
    print(f"  Accuracy             : {accuracy:.4f}  ⚠️  no usar como métrica principal")
    if roc_auc is not None:
        print(f"  ROC-AUC              : {roc_auc:.4f}")
    print(f"\n{classification_report(y_true, y_pred, target_names=['No incendio', 'Incendio'], zero_division=0)}")

    metricas = {
        "f1":        f1,
        "precision": precision,
        "recall":    recall,
        "accuracy":  accuracy,
    }
    if roc_auc is not None:
        metricas["roc_auc"] = roc_auc

    return metricas


# ── Regresión ──────────────────────────────────────────────────────────────────

def evaluar_regresion(y_true, y_pred, nombre="Modelo", en_log=True):
    """
    Calcula y muestra las métricas de regresión.

    La métrica principal es RMSE porque penaliza errores grandes,
    lo cual es importante cuando hay incendios con FRP muy alto.

    Args:
        y_true:  valores reales (en escala log si en_log=True)
        y_pred:  predicciones del modelo (en escala log si en_log=True)
        nombre:  nombre del modelo para mostrar en el resumen
        en_log:  si True, también muestra métricas en MW originales (MW = expm1)

    Returns:
        dict con todas las métricas
    """
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae  = mean_absolute_error(y_true, y_pred)
    r2   = r2_score(y_true, y_pred)

    print(f"\n── Métricas {nombre} ──────────────────────────────")
    print(f"  RMSE (principal): {rmse:.4f}")
    print(f"  MAE             : {mae:.4f}")
    print(f"  R²              : {r2:.4f}")

    metricas = {"rmse": rmse, "mae": mae, "r2": r2}

    # Convertir de vuelta a MW para interpretación real
    if en_log:
        y_true_mw = np.expm1(y_true)
        y_pred_mw = np.expm1(y_pred)
        rmse_mw   = np.sqrt(mean_squared_error(y_true_mw, y_pred_mw))
        mae_mw    = mean_absolute_error(y_true_mw, y_pred_mw)
        print(f"\n  En MW (escala original):")
        print(f"  RMSE: {rmse_mw:.2f} MW")
        print(f"  MAE : {mae_mw:.2f} MW")
        metricas["rmse_mw"] = rmse_mw
        metricas["mae_mw"]  = mae_mw

    return metricas