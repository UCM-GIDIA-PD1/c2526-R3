import sys
import os
import argparse
from modelos import parser
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
import wandb

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import (
    split_simple, split_estratificado, get_pesos_clase
)
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf

load_dotenv()

SEED       = 42
MAX_ITER   = 1000
SOLVER     = "lbfgs"
OUTPUT_DIR = "outputs"

WANDB_ENTITY  = "pd1-c2526-team3"
WANDB_PROJECT = "clasificacion"


# ──────────────────────────────────────────────────────────────────────────────

def entrenar(X_train, y_train, class_weight=None):
    scaler = StandardScaler()
    X_sc = scaler.fit_transform(X_train)

    modelo = LogisticRegression(
        class_weight=class_weight,
        max_iter=MAX_ITER,
        solver=SOLVER,
        random_state=SEED
    )
    modelo.fit(X_sc, y_train)
    return modelo, scaler


def evaluar(modelo, scaler, X, y, nombre="Validación"):
    X_sc   = scaler.transform(X)
    y_pred = modelo.predict(X_sc)
    y_prob = modelo.predict_proba(X_sc)[:, 1]
    return evaluar_clasificacion(y, y_pred, y_prob, nombre)


def analizar_coeficientes(modelo, feature_names):
    coefs = pd.Series(modelo.coef_[0], index=feature_names)
    print("\n── Coeficientes (mayor influencia primero) ──────────")
    for var in coefs.abs().sort_values(ascending=False).index:
        signo = "↑ incendio" if coefs[var] > 0 else "↓ incendio"
        print(f"  {var:25s}: {coefs[var]:+.4f}  ({signo})")
    return coefs


def registrar_wandb(modelo, scaler, metricas_val, metricas_test, coefs,
                    X_train, X_val, y_train, y_val):
    wandb.log({
        "val/f1":        metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall":    metricas_val["recall"],
        "val/roc_auc":   metricas_val.get("roc_auc", 0),
        "test/f1":        metricas_test["f1"],
        "test/precision": metricas_test["precision"],
        "test/recall":    metricas_test["recall"],
        "test/roc_auc":   metricas_test.get("roc_auc", 0),
    })

    wandb.log({f"coef/{var}": val for var, val in coefs.items()})

    X_train_sc = scaler.transform(X_train)
    X_val_sc   = scaler.transform(X_val)
    y_pred_val = modelo.predict(X_val_sc)
    y_prob_val = modelo.predict_proba(X_val_sc)

    wandb.sklearn.plot_classifier(
        modelo,
        X_train_sc, X_val_sc,
        y_train, y_val,
        y_pred_val, y_prob_val,
        labels=["no_incendio", "incendio"],
        model_name="LogisticRegression",
        feature_names=list(coefs.index)
    )


def run_experimento(nombre, X, y, eliminar_correladas):
    print(f"\n{'='*60}")
    print(f"EXPERIMENTO — Split {nombre}")
    print('='*60)

    if nombre == "simple":
        X_train, X_val, X_test, y_train, y_val, y_test = split_simple(X, y)
        class_weight = None
    elif nombre == "estratificado":
        X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)
        class_weight = None
    else:
        X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)
        class_weight = get_pesos_clase(y_train)

    # Entrenamiento y evaluación en validación
    modelo, scaler = entrenar(X_train, y_train, class_weight)
    metricas_val = evaluar(modelo, scaler, X_val, y_val, f"Validación — {nombre}")
    coefs = analizar_coeficientes(modelo, X.columns.tolist())

    # Reentrenamiento con train+val para evaluación final en test
    X_trainval = pd.concat([X_train, X_val])
    y_trainval = pd.concat([y_train, y_val])
    modelo_final, scaler_final = entrenar(X_trainval, y_trainval, class_weight)
    metricas_test = evaluar(modelo_final, scaler_final, X_test, y_test, f"Test — {nombre}")

    config = {
        "modelo":              "regresion_logistica",
        "solver":              SOLVER,
        "max_iter":            MAX_ITER,
        "class_weight":        str(class_weight),
        "split":               nombre,
        "n_features":          X.shape[1],
        "eliminar_correladas": eliminar_correladas,
        "seed":                SEED,
    }
    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        name=f"logistica_{nombre}",
        config=config,
        reinit=True
    )
    registrar_wandb(modelo, scaler, metricas_val, metricas_test, coefs,
                    X_train, X_val, y_train, y_val)
    run.finish()

    return metricas_val, coefs


def generar_graficas(resultados, coefs_dict):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fig = plt.figure(figsize=(16, 10))
    fig.suptitle("Regresión Logística — Clasificación de incendios",
                 fontsize=13, fontweight="bold")
    gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.45, wspace=0.4)

    splits  = list(resultados.keys())
    colores = ["steelblue", "darkorange", "tomato"]

    for i, metrica in enumerate(["f1", "precision", "recall"]):
        ax   = fig.add_subplot(gs[0, i])
        vals = [resultados[s].get(metrica, 0) for s in splits]
        bars = ax.bar(splits, vals, color=colores)
        ax.set_title(metrica.upper())
        ax.set_ylim(0, 1)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, val + 0.01,
                    f"{val:.3f}", ha="center", fontsize=9)

    ax4  = fig.add_subplot(gs[1, 0])
    rocs = [resultados[s].get("roc_auc", 0) for s in splits]
    bars = ax4.bar(splits, rocs, color=colores)
    ax4.set_title("ROC-AUC")
    ax4.set_ylim(0, 1)
    for bar, val in zip(bars, rocs):
        ax4.text(bar.get_x() + bar.get_width() / 2, val + 0.01,
                 f"{val:.3f}", ha="center", fontsize=9)

    mejor     = max(resultados, key=lambda s: resultados[s].get("f1", 0))
    ax5       = fig.add_subplot(gs[1, 1:])
    coefs     = coefs_dict[mejor].sort_values()
    colores_c = ["tomato" if v > 0 else "steelblue" for v in coefs]
    ax5.barh(coefs.index, coefs.values, color=colores_c)
    ax5.axvline(0, color="black", linewidth=0.8)
    ax5.set_title(f"Coeficientes — {mejor} (mejor F1)")
    ax5.set_xlabel("Coeficiente  (rojo = ↑ incendio, azul = ↓ incendio)")

    ruta = os.path.join(OUTPUT_DIR, "logistica_resultados.png")
    plt.savefig(ruta, dpi=150, bbox_inches="tight")
    print(f"\n Gráficas guardadas en: {ruta}")
    plt.close()


# ──────────────────────────────────────────────────────────────────────────────

def main():
    args = parser.initialite_parser()

    if not wf.inicializar_apikey_wandb():
        return

    eliminar = args.eliminar_correladas
    X, y = cargar_dataset_general(eliminar_correladas=eliminar)

    resultados = {}
    coefs_dict = {}

    splits_a_ejecutar = (
        ["simple", "estratificado", "pesos"] if args.split == "todos"
        else [args.split]
    )

    for split in splits_a_ejecutar:
        m, c = run_experimento(split, X, y, eliminar)
        resultados[split] = m
        coefs_dict[split] = c

    if len(resultados) > 1:
        print(f"\n{'='*60}")
        print("RESUMEN COMPARATIVO — Validación")
        print('='*60)
        print(f"  {'Split':<20} {'F1':>8} {'Precision':>10} {'Recall':>8} {'ROC-AUC':>9}")
        print("  " + "-"*57)
        for s, m in resultados.items():
            print(f"  {s:<20} {m['f1']:>8.4f} {m['precision']:>10.4f} "
                  f"{m['recall']:>8.4f} {m.get('roc_auc', 0):>9.4f}")
        mejor = max(resultados, key=lambda s: resultados[s]["f1"])
        print(f"\n  Mejor F1: {mejor}  ({resultados[mejor]['f1']:.4f})")

    if not args.no_graficas:
        generar_graficas(resultados, coefs_dict)


if __name__ == "__main__":
    main()