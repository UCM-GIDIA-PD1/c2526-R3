import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

import wandb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
)

from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

# Configuración
WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "Regresion_Logistica"
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    """
    Ejecuta la evaluación definitiva sobre el conjunto de TEST 
    usando los mejores parámetros encontrados en el Sweep.
    """
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
        "method": "grid",
        "parameters": {
            "penalty": {"values": [hiperparametros["penalty"]]},
            "class_weight": {"values": [hiperparametros["class_weight"]]},
            "umbral": {"values": [hiperparametros["umbral"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train_full)
    X_test_sc = scaler.transform(X_test)

    clf = LogisticRegression(
        penalty=config.penalty,
        class_weight=config.class_weight,
        solver='saga', 
        max_iter=5000,
        random_state=SEED,
        n_jobs=-1
    )
    clf.fit(X_train_sc, y_train_full)

    y_prob_test = clf.predict_proba(X_test_sc)
    y_pred_test = (y_prob_test[:, 1] >= config.umbral).astype(int)

    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — Logística")

    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0)
    })

    coefs = pd.Series(clf.coef_[0], index=X_train_full.columns.tolist())
    for name, val in coefs.items():
        wandb.log({f"coef/{name}": val})

    wandb.sklearn.plot_classifier(
        clf, X_train_sc, X_test_sc, y_train_full, y_test, y_pred_test, y_prob_test,
        labels=["no_incendio", "incendio"],
        model_name="LogisticRegression",
        feature_names=X_train_full.columns.tolist(),
    )

    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())
    
    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1
    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    f2_cv_scores, f2_cv_train = [], []
    f1_cv_scores, f1_cv_train = [], []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train, X_fold_val = X_train_full.iloc[train_idx], X_train_full.iloc[val_idx]
        y_fold_train, y_fold_val = y_train_full.iloc[train_idx], y_train_full.iloc[val_idx]

        scaler = StandardScaler()
        X_fold_train_sc = scaler.fit_transform(X_fold_train)
        X_fold_val_sc = scaler.transform(X_fold_val)

        clf = LogisticRegression(
            C=config.C, penalty=config.penalty, class_weight=config.class_weight,
            solver='saga', max_iter=5000, random_state=SEED
        )
        clf.fit(X_fold_train_sc, y_fold_train)

        y_val_prob = clf.predict_proba(X_fold_val_sc)[:, 1]
        y_fold_pred = (y_val_prob >= config.umbral).astype(int)
        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        f1_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=1, zero_division=0))

        y_train_prob = clf.predict_proba(X_fold_train_sc)[:, 1]
        y_train_pred = (y_train_prob >= config.umbral).astype(int)
        f2_cv_train.append(fbeta_score(y_fold_train, y_train_pred, beta=2, zero_division=0))
        f1_cv_train.append(fbeta_score(y_fold_val, y_fold_pred, beta=1, zero_division=0))

    wandb.log({
        "train/f2_mean_cv": float(np.mean(f2_cv_train)),
        "val/f2_mean_cv": float(np.mean(f2_cv_scores)),
        "train/f1_mean_cv": float(np.mean(f2_cv_train)),
        "val/f1_mean_cv": float(np.mean(f2_cv_scores))
    })
    run.finish()

def inicializar():
    if not wf.inicializar_apikey_wandb(): return
    X, y = cargar_dataset_general(eliminar_correladas=False)
    X, y = pers.pregunta_PCA()
    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)
    return X_train_full, X_test, y_train_full, y_test

def clasificacion(metodo_elegido, metrica_elegida):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    iters, nombre = pers.pregunta_iters_nombre()

    if metodo_elegido == "grid":
        params = {
            "penalty": {"values": ["l1", "l2"]},
            "class_weight": {"values": ["balanced", None]},
            "umbral": {"values": [0.25, 0.35, 0.45]}
        }
    else: 
        params = {
            "penalty": {"values": ["l1", "l2", None]},
            "class_weight": {"values": ["balanced", None]},
            "umbral": {"distribution": "uniform", "min": 0.1, "max": 0.5}
        }

    metric_name = "val/f2_mean_cv" if "f2" in metrica_elegida.lower() else "val/f1_mean_cv"

    sweep_config = {
        "name": f"Logistica-{metodo_elegido}-{metrica_elegida}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": "maximize"},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    
    wandb.agent(sweep_id, function=lambda: entrenamiento(X_train_full, y_train_full, nombre), count=iters)

if __name__ == "__main__":
    metodo = input("\n Método (grid o random): ").lower()
    metrica = input("\n Métrica a optimizar (f1 o f2): ").lower()
    clasificacion(metodo, metrica)