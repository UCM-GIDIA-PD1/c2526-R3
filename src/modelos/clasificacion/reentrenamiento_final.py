"""
reentrenamiento_final.py
------------------------
Reentrena el mejor modelo XGBoost de clasificación usando todos los datos
de fase 3 (MINI.parquet) + los nuevos datos de 2026 (final_2026.parquet),
y evalúa el modelo sobre los datos nuevos.

Los hiperparámetros corresponden al mejor run del sweep XGBoost-random-f2-Sweep
(run: v5cmrozh, sweep: 3pdmt88q).

Uso:
    uv run python src/modelos/clasificacion/reentrenamiento_final.py \
        --datos_fase3 <ruta_MINI.parquet> \
        --datos_nuevos <ruta_final_2026.parquet> \
        --output_dir <carpeta_salida>
"""

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    fbeta_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

# ──────────────────────────────────────────────
# Hiperparámetros del mejor run (v5cmrozh)
# sweep: 3pdmt88q — XGBoost-random-f2-Sweep
# ──────────────────────────────────────────────

MEJORES_HIPERPARAMETROS = {
    "n_estimators": 2000,
    "learning_rate": 0.17971228299068043,
    "max_depth": 4,
    "subsample": 0.8142987782803428,
    "colsample_bytree": 0.8311621901655458,
    "min_child_weight": 1,
    "gamma": 0,
}

UMBRAL = 0.35189353387841105
SEED = 42

# Columnas que el pipeline original elimina por correlación alta
COLS_ELIMINAR = ["porcentaje", "temp_max", "temp_min", "NDVI", "pressure_mean"]
COLS_NO_FEATURES = ["final", "date", "_year"]


# ──────────────────────────────────────────────
# Carga y preparación de datos
# ──────────────────────────────────────────────

def preparar_datos(ruta: str) -> tuple[pd.DataFrame, pd.Series]:
    """Carga un parquet y devuelve (X, y) aplicando el mismo preprocesado que en fase 3."""
    df = pd.read_parquet(ruta)
    df["date"] = pd.to_datetime(df["date"])
    df = df.drop(columns=COLS_ELIMINAR, errors="ignore")
    y = df["final"]
    X = df.drop(columns=[c for c in COLS_NO_FEATURES if c in df.columns], errors="ignore")
    return X, y


def calcular_ratio_clases(y: pd.Series) -> float:
    counts = y.value_counts()
    return counts[0] / counts[1]


# ──────────────────────────────────────────────
# Entrenamiento
# ──────────────────────────────────────────────

def entrenar(X_train: pd.DataFrame, y_train: pd.Series) -> xgb.XGBClassifier:
    ratio = calcular_ratio_clases(y_train)
    print(f"\n  Ratio de clases (scale_pos_weight): {ratio:.2f}")
    print(f"  Incendios en train: {y_train.sum():,}  |  No incendios: {(y_train==0).sum():,}")

    clf = xgb.XGBClassifier(
        **MEJORES_HIPERPARAMETROS,
        scale_pos_weight=ratio,
        random_state=SEED,
        eval_metric="aucpr",
        n_jobs=-1,
    )

    print("\nEntrenando modelo XGBoost...")
    clf.fit(X_train, y_train)
    print("Entrenamiento completado.")
    return clf


# ──────────────────────────────────────────────
# Evaluación
# ──────────────────────────────────────────────

def evaluar(clf: xgb.XGBClassifier, X: pd.DataFrame, y: pd.Series, nombre: str) -> dict:
    y_prob = clf.predict_proba(X)[:, 1]
    y_pred = (y_prob >= UMBRAL).astype(int)

    metricas = {
        "accuracy":  round(accuracy_score(y, y_pred), 4),
        "f1":        round(f1_score(y, y_pred, zero_division=0), 4),
        "f2":        round(fbeta_score(y, y_pred, beta=2, zero_division=0), 4),
        "precision": round(precision_score(y, y_pred, zero_division=0), 4),
        "recall":    round(recall_score(y, y_pred, zero_division=0), 4),
        "auc_roc":   round(roc_auc_score(y, y_prob), 4),
    }

    cm = confusion_matrix(y, y_pred)

    print(f"\n{'='*50}")
    print(f"MÉTRICAS — {nombre}")
    print(f"{'='*50}")
    for k, v in metricas.items():
        print(f"  {k:<12}: {v}")
    print(f"\n  Matriz de confusión:")
    print(f"    TN={cm[0,0]}  FP={cm[0,1]}")
    print(f"    FN={cm[1,0]}  TP={cm[1,1]}")

    return metricas


def comparar_metricas(metricas_nuevos: dict) -> None:
    """Imprime comparativa entre métricas de test fase 3 (memoria) y métricas sobre datos nuevos."""
    test_fase3_memoria = {
        "accuracy":  0.9547,
        "f1":        0.3724,
        "f2":        0.3775,
        "precision": 0.3641,
        "recall":    0.3810,
    }

    print(f"\n{'='*62}")
    print("COMPARATIVA: Test Fase 3 (memoria) vs. Datos nuevos 2026")
    print(f"{'='*62}")
    print(f"  {'Métrica':<12} {'Fase 3 test':>12} {'2026':>12} {'Diferencia':>12}")
    print(f"  {'-'*52}")
    for k in ["accuracy", "f1", "f2", "precision", "recall"]:
        v3 = test_fase3_memoria[k]
        vn = metricas_nuevos[k]
        diff = round(vn - v3, 4)
        signo = "+" if diff > 0 else ""
        print(f"  {k:<12} {v3:>12} {vn:>12} {signo}{diff:>11}")


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Reentrenamiento final XGBoost clasificación — Fase 4")
    parser.add_argument(
        "--datos_fase3",
        type=str,
        default="data/MINI.parquet",
        help="Ruta al parquet de fase 3 (MINI.parquet)",
    )
    parser.add_argument(
        "--datos_nuevos",
        type=str,
        default="data/final_2026.parquet",
        help="Ruta al parquet de datos nuevos (final_2026.parquet)",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="resultados/reentrenamiento_fase4",
        help="Carpeta donde guardar el modelo y las métricas",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Carga ──
    print("\n" + "="*50)
    print("CARGANDO DATOS")
    print("="*50)

    print(f"\nFase 3: {args.datos_fase3}")
    X_fase3, y_fase3 = preparar_datos(args.datos_fase3)
    print(f"  → {X_fase3.shape[0]:,} registros | {y_fase3.sum():,} incendios ({y_fase3.mean()*100:.2f}%)")

    print(f"\nNuevos datos 2026: {args.datos_nuevos}")
    X_nuevos, y_nuevos = preparar_datos(args.datos_nuevos)
    print(f"  → {X_nuevos.shape[0]:,} registros | {y_nuevos.sum():,} incendios ({y_nuevos.mean()*100:.2f}%)")

    # ── Merge: fase3 + 2026 para entrenamiento ──
    print(f"\n{'='*50}")
    print("COMBINANDO DATASETS PARA ENTRENAMIENTO")
    print(f"{'='*50}")

    cols_comunes = [c for c in X_fase3.columns if c in X_nuevos.columns]
    X_nuevos_alineado = X_nuevos[cols_comunes]
    X_fase3_alineado = X_fase3[cols_comunes]

    X_train_total = pd.concat([X_fase3_alineado, X_nuevos_alineado], ignore_index=True)
    y_train_total = pd.concat([y_fase3, y_nuevos], ignore_index=True)

    print(f"  Dataset combinado: {X_train_total.shape[0]:,} registros")
    print(f"  Incendios totales: {y_train_total.sum():,} ({y_train_total.mean()*100:.2f}%)")
    print(f"  Features usadas ({len(cols_comunes)}): {cols_comunes}")

    # ── Entrenamiento ──
    print(f"\n{'='*50}")
    print("ENTRENAMIENTO (Fase 3 + 2026)")
    print(f"{'='*50}")
    clf = entrenar(X_train_total, y_train_total)

    # ── Evaluación sobre datos nuevos ──
    metricas_nuevos = evaluar(clf, X_nuevos_alineado, y_nuevos, "Datos nuevos 2026")

    # ── Evaluación sobre datos fase 3 ──
    evaluar(clf, X_fase3_alineado, y_fase3, "Datos fase 3 completos")

    # ── Comparativa ──
    comparar_metricas(metricas_nuevos)

    # ── Guardar modelo ──
    ruta_modelo = output_dir / "xgboost_clasificacion_final.pkl"
    joblib.dump(clf, ruta_modelo)
    print(f"\nModelo guardado en: {ruta_modelo}")

    # ── Guardar métricas ──
    resultados = {
        "descripcion": "Modelo reentrenado con datos fase 3 + datos 2026",
        "datos_entrenamiento": {
            "fase3_registros": int(X_fase3.shape[0]),
            "fase3_incendios": int(y_fase3.sum()),
            "nuevos_2026_registros": int(X_nuevos.shape[0]),
            "nuevos_2026_incendios": int(y_nuevos.sum()),
            "total_registros": int(X_train_total.shape[0]),
            "total_incendios": int(y_train_total.sum()),
        },
        "hiperparametros": MEJORES_HIPERPARAMETROS,
        "umbral": UMBRAL,
        "sweep_id": "3pdmt88q",
        "run_id": "v5cmrozh",
        "metricas_datos_nuevos_2026": metricas_nuevos,
        "metricas_test_fase3_memoria": {
            "accuracy": 0.9547,
            "f1": 0.3724,
            "f2": 0.3775,
            "precision": 0.3641,
            "recall": 0.3810,
        },
    }
    ruta_json = output_dir / "resultados_clasificacion.json"
    with open(ruta_json, "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)
    print(f"étricas guardadas en: {ruta_json}")
    print(f"\nTodo completado. Archivos en: {output_dir}")


if __name__ == "__main__":
    main()
