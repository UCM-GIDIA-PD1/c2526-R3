import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score

import wandb
import xgboost as xgb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SWEEP_PATH = Path(__file__).with_name("xgboost_sweep.yaml")
SEED = 42
NUM_IT = 0

def xgboost_entrenamiento(X_train_full, X_test, y_train_full, y_test, detallados=False, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    clf = xgb.XGBClassifier(
        n_estimators=getattr(config, 'n_estimators', 1000),
        learning_rate=getattr(config, 'learning_rate', 0.1),
        max_depth=getattr(config, 'max_depth', 6),
        subsample=getattr(config, 'subsample', 1.0),
        colsample_bytree=getattr(config, 'colsample_bytree', 1.0),
        random_state=SEED,
        eval_metric="logloss",
        n_jobs=-1
    )

    cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
    f2_cv_scores = []
    f1_cv_scores = []
    recall_cv_scores = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        clf.fit(X_fold_train, y_fold_train)
        y_fold_pred = clf.predict(X_fold_val)

        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))

    f2_score_mean = np.mean(f2_cv_scores)
    f1_score_mean = np.mean(f1_cv_scores)
    recall_score_mean = np.mean(recall_cv_scores)

    clf.fit(X_train_full, y_train_full)

    y_pred_test = clf.predict(X_test)
    y_prob_test = clf.predict_proba(X_test)[:, 1]
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test, "Test — XGBoost")

    wandb.log({
        "val/f1_mean_cv": float(f1_score_mean),
        "val/f2_mean_cv": float(f2_score_mean),
        "val/recall_mean_cv": float(recall_score_mean),
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0)
    })

    if detallados:
        wandb.sklearn.plot_classifier(
            clf,
            X_train_full,
            X_test,
            y_train_full,
            y_test,
            y_pred_test,
            clf.predict_proba(X_test),
            labels=["no_incendio", "incendio"],
            model_name="XGBoostClassifier",
            feature_names=X_train_full.columns.tolist(),
        )
        plot_roc(y_test, clf.predict_proba(X_test), ["no_incendio", "incendio"])
        plot_precision_recall(y_test, clf.predict_proba(X_test), ["no_incendio", "incendio"])
        plot_feature_importances(clf)
        wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()

def clasificacion():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cargar_dataset_general(eliminar_correladas=False)

    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, date_col='date', test_size=0.2)

    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    detallado = input("¿Quieres ver los resultados detallados (s/n): ")
    detallados = detallado.lower() == "s"

    def entrenamiento():
        xgboost_entrenamiento(X_train_full, X_test, y_train_full, y_test, detallados, nombre)

    sweep_id = wf.crear_sweep_id(WANDB_PROJECT, SWEEP_PATH)

    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    clasificacion()