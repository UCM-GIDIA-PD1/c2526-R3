import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score

import wandb
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
from sklearn.ensemble import RandomForestClassifier

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "sweep_random_forest_umbral_smote"
SWEEP_PATH = Path(__file__).with_name("randomforest_sweep.yaml")
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
            "min_samples_leaf": {"values": [hiperparametros["min_samples_leaf"]]},
            "criterion": {"values": [hiperparametros["criterion"]]},
            "class_weight": {"values": [hiperparametros["class_weight"]]},
            "max_features": {"values": [hiperparametros["max_features"]]},
            "umbral": {"values": [hiperparametros["umbral"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    clf = RandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        class_weight=config.class_weight,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    clf.fit(X_train_full, y_train_full)

    y_prob_test = clf.predict_proba(X_test)
    y_pred_test = (y_prob_test[:, 1] >= config.umbral).astype(int)
    
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — RandomForest")

    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0),
        "test/f1_5_score": fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)
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
        y_prob_test,
        labels=["no_incendio", "incendio"],
        model_name="RandomForest",
        feature_names=X_train_full.columns.tolist(),
    )
    
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    clf = RandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        class_weight=config.class_weight,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
    f2_cv_scores, f1_5_cv_scores = [], []
    f2_cv_train, f1_5_cv_train = [], []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train, X_fold_val = X_train_full.iloc[train_idx], X_train_full.iloc[val_idx]
        y_fold_train, y_fold_val = y_train_full.iloc[train_idx], y_train_full.iloc[val_idx]

        clf.fit(X_fold_train, y_fold_train)
        
        y_val_prob = clf.predict_proba(X_fold_val)[:, 1]
        y_val_pred = (y_val_prob >= config.umbral).astype(int)
    
        y_t_prob = clf.predict_proba(X_fold_train)[:, 1]
        y_t_pred = (y_t_prob >= config.umbral).astype(int)
        f2_cv_train.append(fbeta_score(y_fold_train, y_t_pred, beta=2, zero_division=0))
        f1_5_cv_train.append(fbeta_score(y_fold_train, y_t_pred, beta=1.5, zero_division=0))

        f2_cv_scores.append(fbeta_score(y_fold_val, y_val_pred, beta=2, zero_division=0))
        f1_5_cv_scores.append(fbeta_score(y_fold_val, y_val_pred, beta=1.5, zero_division=0))

    wandb.log({
        "train/f2_mean_cv": np.mean(f2_cv_train),
        "train/f1_5_mean_cv": np.mean(f1_5_cv_train),
        "val/f2_mean_cv": np.mean(f2_cv_scores), 
        "val/f1_5_mean_cv": np.mean(f1_5_cv_scores)
    })

    clf.fit(X_train_full, y_train_full)
    plot_feature_importances(clf)
    y_pred_train = (clf.predict_proba(X_train_full)[:, 1] >= config.umbral).astype(int)
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