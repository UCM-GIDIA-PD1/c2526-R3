"""
regresion_logistica.py
----------------------
Modelo de regresión logística para clasificación binaria de incendios.
Problema: predecir si se producirá un incendio (1) o no (0).

Uso:
    uv run python src/modelos/clasificacion/regresion_logistica.py
    uv run python src/modelos/clasificacion/regresion_logistica.py --split simple
    uv run python src/modelos/clasificacion/regresion_logistica.py --split estratificado
    uv run python src/modelos/clasificacion/regresion_logistica.py --split pesos

Descripción del modelo:
    La regresión logística modela la probabilidad de incendio como una función
    sigmoide de las variables predictoras. Con un umbral de decisión (por defecto
    0.5), clasifica como incendio si P(incendio) > umbral.

    Hipótesis que asume:
        - No multicolinealidad severa entre variables (ya corregida en carga_datos)
        - Relación lineal entre log-odds y las variables predictoras
        - Observaciones independientes entre sí

    Transformaciones aplicadas:
        - StandardScaler: escala todas las variables a media 0 y std 1.
          Necesario para que el optimizador converja bien y los coeficientes
          sean comparables entre variables.
"""

import sys
import os
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

# Añadir src/ al path para imports
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import (
    split_simple, split_estratificado,
    get_pesos_clase, calcular_sample_weights
)
from modelos.utils.metricas import evaluar_clasificacion

# ── Configuración ──────────────────────────────────────────────────────────────

SEED           = 42
MAX_ITER       = 1000   # iteraciones máximas del optimizador
SOLVER         = "lbfgs"
OUTPUT_DIR     = "outputs"


# ── Entrenamiento ──────────────────────────────────────────────────────────────

def entrenar_logistica(X_train, y_train, class_weight=None):
    """
    Entrena un modelo de regresión logística con escalado previo.

    Args:
        X_train:      features de entrenamiento
        y_train:      target de entrenamiento
        class_weight: None, 'balanced', o dict {0: w0, 1: w1}

    Returns:
        modelo:  LogisticRegression entrenado
        scaler:  StandardScaler ajustado (para transformar val y test)
    """
    # Escalar SOLO con datos de train (evitar data leakage)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)

    modelo = LogisticRegression(
        class_weight=class_weight,
        max_iter=MAX_ITER,
        solver=SOLVER,
        random_state=SEED
    )
    modelo.fit(X_train_scaled, y_train)

    return modelo, scaler


def evaluar_split(modelo, scaler, X_val, y_val, nombre="Validación"):
    """Escala X_val y evalúa el modelo."""
    X_val_scaled = scaler.transform(X_val)
    y_pred = modelo.predict(X_val_scaled)
    y_prob = modelo.predict_proba(X_val_scaled)[:, 1]
    return evaluar_clasificacion(y_val, y_pred, y_prob, nombre)


# ── Análisis de coeficientes ───────────────────────────────────────────────────

def analizar_coeficientes(modelo, feature_names):
    """
    Muestra los coeficientes del modelo ordenados por importancia.
    Un coeficiente positivo alto → mayor probabilidad de incendio.
    Un coeficiente negativo → menor probabilidad de incendio.
    """
    coefs = pd.Series(modelo.coef_[0], index=feature_names)
    coefs_abs = coefs.abs().sort_values(ascending=False)

    print("\n── Importancia de variables (coeficientes) ────────")
    for var in coefs_abs.index:
        signo = "↑ incendio" if coefs[var] > 0 else "↓ incendio"
        print(f"  {var:25s}: {coefs[var]:+.4f}  ({signo})")

    return coefs


# ── Visualizaciones ────────────────────────────────────────────────────────────

def generar_graficas(resultados, coefs_dict, feature_names):
    """
    Genera una figura comparando los 3 tipos de muestreo y los coeficientes.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fig = plt.figure(figsize=(16, 10))
    fig.suptitle("Regresión Logística — Clasificación de incendios", 
                 fontsize=13, fontweight="bold")
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.4)

    metricas_nombres = ["f1", "precision", "recall", "roc_auc"]
    splits = list(resultados.keys())
    colores = ["steelblue", "darkorange", "tomato"]

    # 1-3. Barras de métricas por split
    for i, metrica in enumerate(metricas_nombres[:3]):
        ax = fig.add_subplot(gs[0, i])
        valores = [resultados[s].get(metrica, 0) for s in splits]
        bars = ax.bar(splits, valores, color=colores)
        ax.set_title(metrica.upper().replace("_", "-"))
        ax.set_ylim(0, 1)
        ax.set_ylabel("Valor")
        for bar, val in zip(bars, valores):
            ax.text(bar.get_x() + bar.get_width()/2, val + 0.01,
                    f"{val:.3f}", ha="center", fontsize=9)

    # 4. ROC-AUC comparación
    ax4 = fig.add_subplot(gs[1, 0])
    valores_roc = [resultados[s].get("roc_auc", 0) for s in splits]
    bars = ax4.bar(splits, valores_roc, color=colores)
    ax4.set_title("ROC-AUC")
    ax4.set_ylim(0, 1)
    for bar, val in zip(bars, valores_roc):
        ax4.text(bar.get_x() + bar.get_width()/2, val + 0.01,
                 f"{val:.3f}", ha="center", fontsize=9)

    # 5. Coeficientes del mejor modelo (estratificado + pesos)
    ax5 = fig.add_subplot(gs[1, 1:])
    mejor_split = max(resultados, key=lambda s: resultados[s].get("f1", 0))
    coefs = coefs_dict[mejor_split]
    coefs_sorted = coefs.sort_values()
    colores_coef = ["tomato" if v > 0 else "steelblue" for v in coefs_sorted]
    ax5.barh(coefs_sorted.index, coefs_sorted.values, color=colores_coef)
    ax5.axvline(0, color="black", linewidth=0.8)
    ax5.set_title(f"Coeficientes — {mejor_split} (mejor F1)")
    ax5.set_xlabel("Coeficiente (rojo=↑ incendio, azul=↓ incendio)")

    ruta = os.path.join(OUTPUT_DIR, "logistica_resultados.png")
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"\n✅ Gráficas guardadas en: {ruta}")
    plt.close()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Regresión logística para clasificación de incendios IgnisAI"
    )
    parser.add_argument(
        "--split",
        choices=["simple", "estratificado", "pesos", "todos"],
        default="todos",
        help="Estrategia de muestreo a usar (default: todos)"
    )
    parser.add_argument(
        "--no-graficas", action="store_true",
        help="No generar gráficas"
    )
    args = parser.parse_args()

    # ── 1. Cargar datos ────────────────────────────────────────────────────────
    X, y = cargar_dataset_general(eliminar_correladas=False)
    feature_names = X.columns.tolist()

    resultados  = {}
    coefs_dict  = {}

    # ── 2. Split simple ────────────────────────────────────────────────────────
    if args.split in ("simple", "todos"):
        print("\n" + "="*60)
        print("EXPERIMENTO 1 — Split simple (sin estratificación)")
        print("="*60)
        X_train, X_val, X_test, y_train, y_val, y_test = split_simple(X, y)

        modelo, scaler = entrenar_logistica(X_train, y_train, class_weight=None)
        metricas_val = evaluar_split(modelo, scaler, X_val, y_val, "Validación — Simple")

        resultados["simple"] = metricas_val
        coefs_dict["simple"] = analizar_coeficientes(modelo, feature_names)

    # ── 3. Split estratificado ─────────────────────────────────────────────────
    if args.split in ("estratificado", "todos"):
        print("\n" + "="*60)
        print("EXPERIMENTO 2 — Split estratificado")
        print("="*60)
        X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

        modelo, scaler = entrenar_logistica(X_train, y_train, class_weight=None)
        metricas_val = evaluar_split(modelo, scaler, X_val, y_val, "Validación — Estratificado")

        resultados["estratificado"] = metricas_val
        coefs_dict["estratificado"] = analizar_coeficientes(modelo, feature_names)

    # ── 4. Split estratificado + pesos ─────────────────────────────────────────
    if args.split in ("pesos", "todos"):
        print("\n" + "="*60)
        print("EXPERIMENTO 3 — Split estratificado + pesos de clase")
        print("="*60)
        X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

        pesos = get_pesos_clase(y_train)
        modelo, scaler = entrenar_logistica(X_train, y_train, class_weight=pesos)
        metricas_val = evaluar_split(modelo, scaler, X_val, y_val, "Validación — Pesos")

        resultados["pesos"] = metricas_val
        coefs_dict["pesos"] = analizar_coeficientes(modelo, feature_names)

    # ── 5. Resumen comparativo ─────────────────────────────────────────────────
    if len(resultados) > 1:
        print("\n" + "="*60)
        print("RESUMEN COMPARATIVO — Validación")
        print("="*60)
        print(f"  {'Split':<20} {'F1':>8} {'Precision':>10} {'Recall':>8} {'ROC-AUC':>9}")
        print("  " + "-"*57)
        for split_nombre, m in resultados.items():
            roc = m.get('roc_auc', float('nan'))
            print(f"  {split_nombre:<20} {m['f1']:>8.4f} {m['precision']:>10.4f} "
                  f"{m['recall']:>8.4f} {roc:>9.4f}")

        mejor = max(resultados, key=lambda s: resultados[s]["f1"])
        print(f"\n  ✅ Mejor F1 en validación: {mejor} ({resultados[mejor]['f1']:.4f})")

    # ── 6. Gráficas ────────────────────────────────────────────────────────────
    if not args.no_graficas and len(resultados) > 0:
        generar_graficas(resultados, coefs_dict, feature_names)


if __name__ == "__main__":
    main()