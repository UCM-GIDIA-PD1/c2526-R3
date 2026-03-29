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

from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers
from imblearn.ensemble import BalancedRandomForestClassifier

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "balancedRandomForestClassifier"
SWEEP_PATH = Path(__file__).with_name("balanced_random_forest.yaml")
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
            "method": "grid",
            "parameters": {
                "max_depth": {"values": [hiperparametros["max_depth"]]},
                "n_estimators": {"values": [hiperparametros["n_estimators"]]},
                "min_samples_split": {"values": [hiperparametros["min_samples_split"]]},
                "min_samples_leaf": {"values": [hiperparametros["min_samples_leaf"]]}
            }
        }
        
    sweep_id_final = wandb.sweep(config_final, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    clf = BalancedRandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        sampling_strategy='auto',
        replacement=False,
        random_state=42,
        n_jobs=-1,
    )

    clf.fit(X_train_full, y_train_full)

    y_pred_test = clf.predict(X_test)
    y_prob_test = clf.predict_proba(X_test)
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — BalancedRF")

    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0)
    })

    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    wandb.sklearn.plot_classifier(
            clf,
            X_train_full,
            X_test,
            y_train_full,
            y_test,
            y_pred_test,
            clf.predict_proba(X_test),
            labels=["no_incendio", "incendio"],
            model_name="balancedRandomForest",
            feature_names=X_train_full.columns.tolist(),
        )
    
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()



def entrenamiento(X_train_full, y_train_full, nombre = None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    clf = BalancedRandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        sampling_strategy='auto',
        replacement=False,
        random_state=SEED,
        n_jobs=-1,
    )

    cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
    f2_cv_scores = []
    f1_cv_scores = []
    f1_cv_scores_train = []
    f2_cv_scores_train = []
    recall_cv_scores_train = []
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

        y_fold_pred2 = clf.predict(X_fold_train)
        f2_cv_scores_train.append(fbeta_score(y_fold_train, y_fold_pred2, beta=2, zero_division=0))
        f1_cv_scores_train.append(f1_score(y_fold_train, y_fold_pred2, zero_division=0))
        recall_cv_scores_train.append(recall_score(y_fold_train, y_fold_pred2, zero_division=0))
        

    f2_score_mean = np.mean(f2_cv_scores)
    f1_score_mean = np.mean(f1_cv_scores)
    recall_score_mean = np.mean(recall_cv_scores)

    f2_score_mean_train = np.mean(f2_cv_scores_train)
    f1_score_mean_train = np.mean(f1_cv_scores_train)
    recall_score_mean_train = np.mean(recall_cv_scores_train)

    wandb.log({
        "train/f1_mean_cv": float(f1_score_mean_train),
        "train/f2_mean_cv": float(f2_score_mean_train),
        "train/recall_mean_cv": float(recall_score_mean_train),
        "val/f1_mean_cv": float(f1_score_mean),
        "val/f2_mean_cv": float(f2_score_mean),
        "val/recall_mean_cv": float(recall_score_mean),
    })

    clf.fit(X_train_full, y_train_full)
    plot_feature_importances(clf)

    y_pred_train = clf.predict(X_train_full)
    y_prob_train = clf.predict_proba(X_train_full)

    plot_roc(y_train_full, y_prob_train)
    plot_precision_recall(y_train_full, y_prob_train)
    wf.matriz_confusion_feature_importance(clf, y_pred_train, y_train_full, X_train_full.columns.tolist())

    run.finish()


def inicializar():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cargar_dataset_general(eliminar_correladas=False)

    X, y = pers.pregunta_PCA()

    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, date_col='date', test_size=0.2)

    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test


def clasificacion():
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    iters, nombre = pers.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train_full, y_train_full, nombre)

    sweep_id = wf.crear_sweep_id(WANDB_PROJECT, SWEEP_PATH)

    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    clasificacion()