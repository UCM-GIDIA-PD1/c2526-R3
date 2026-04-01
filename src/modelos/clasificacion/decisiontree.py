import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import fbeta_score, recall_score, f1_score
from sklearn.metrics import confusion_matrix

import wandb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

import modelos.utils.carga_datos as cg
import modelos.utils.particiones as part
import modelos.utils.metricas as met
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "decision_tree_clasificacion"
SWEEP_PATH = Path(__file__).with_name("decisiontree_sweep.yaml")
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo, X_train_full, X_test, y_train_full, y_test):
    config_final = {
        "method": metodo,
        "parameters": {
            "max_depth": {"values": [hiperparametros["max_depth"]]},
            "min_samples_split": {"values": [hiperparametros["min_samples_split"]]},
            "min_samples_leaf": {"values": [hiperparametros["min_samples_leaf"]]},
            "criterion": {"values": [hiperparametros["criterion"]]},
            "max_features": {"values": [hiperparametros["max_features"]]},
            "class_weight": {"values": [hiperparametros["class_weight"]]},
            "umbral": {"values": [hiperparametros["umbral"]]}
        }
    }
    sweep_id_final = wandb.sweep(config_final, project=WANDB_PROJECT, entity=WANDB_ENTITY)
    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)
    wandb.agent(sweep_id_final, function=agente_final, count=1)

def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config
    max_f = None if config.max_features == "None" else config.max_features

    clf = DecisionTreeClassifier(
        max_depth=config.max_depth, min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf, criterion=config.criterion,
        max_features=max_f, class_weight=config.class_weight, random_state=SEED
    )
    clf.fit(X_train_full, y_train_full)

    y_prob_test = clf.predict_proba(X_test)
    y_pred_test = (y_prob_test[:, 1] >= config.umbral).astype(int)
    
    metricas_test = met.evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — DecisionTree")
    wandb.log({
        "test/f1": float(metricas_test["f1"]), "test/recall": float(metricas_test["recall"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0),
        "test/f1_score": fbeta_score(y_test, y_pred_test, zero_division=0)
    })
    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()

def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1
    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config
    max_f = None if config.max_features == "None" else config.max_features

    clf = DecisionTreeClassifier(
        max_depth=config.max_depth, min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf, criterion=config.criterion,
        max_features=max_f, class_weight=config.class_weight, random_state=SEED
    )
    cv_generator = part.generador_cv(tipo_cv='temporal', n_splits=4, seed=SEED)
    f2_cv_scores, f1_cv_scores = [], []
    f2_cv_train, f1_cv_train = [], []
    tns, fps, fns, tps = [], [], [], []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train, X_fold_val = X_train_full.iloc[train_idx], X_train_full.iloc[val_idx]
        y_fold_train, y_fold_val = y_train_full.iloc[train_idx], y_train_full.iloc[val_idx]
        
        clf.fit(X_fold_train, y_fold_train)
        
        y_v_prob = clf.predict_proba(X_fold_val)[:, 1]
        y_v_pred = (y_v_prob >= config.umbral).astype(int)
        f2_cv_scores.append(fbeta_score(y_fold_val, y_v_pred, beta=2, zero_division=0))
        f1_cv_scores.append(fbeta_score(y_fold_val, y_v_pred, beta = 1, zero_division=0))

        y_t_prob = clf.predict_proba(X_fold_train)[:, 1]
        y_t_pred = (y_t_prob >= config.umbral).astype(int)
        f2_cv_train.append(fbeta_score(y_fold_train, y_t_pred, beta=2, zero_division=0))
        f1_cv_train.append(fbeta_score(y_fold_train, y_t_pred, beta = 1))

        cm = confusion_matrix(y_fold_val, y_v_pred)

        tns.append(cm[0,0])
        fps.append(cm[0,1])
        fns.append(cm[1,0])
        tps.append(cm[1,1])

    wandb.log({
        "train/f2_mean_cv": np.mean(f2_cv_train),
        "train/f1_mean_cv": np.mean(f1_cv_train),
        "val/f2_mean_cv": np.mean(f2_cv_scores), 
        "val/f1_mean_cv": np.mean(f1_cv_scores),
        "val/tn_mean": np.mean(tns),
        "val/fp_mean": np.mean(fps),
        "val/fn_mean": np.mean(fns),
        "val/tp_mean": np.mean(tps)
    })
    run.finish()

def inicializar():
    if not wf.inicializar_apikey_wandb(): sys.exit()
    X, y = cg.cargar_dataset_general()
    X, y = pers.pregunta_PCA()
    X_train_full, X_test, y_train_full, y_test = part.split_temporal(X, y)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)
    return X_train_full, X_test, y_train_full, y_test

def clasificacion(metodo_elegido):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    iters, nombre = pers.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train_full, y_train_full, nombre)

    if metodo_elegido == "grid":
        params = {
            "max_depth": {"values": [5, 15, 25, 35]},
            "min_samples_leaf": {"values": [1, 5, 10, 20]},
            "min_samples_split": {"values": [2, 8, 18]},
            "criterion": {"values": ["gini", "entropy"]},
            "max_features": {"values": ["sqrt", "log2", None]},
            "umbral": {"values": [0.3, 0.45, 0.6]},
            "class_weight": {"values": ["balanced", None]}
        }
    else: 
        params = {
            "max_depth": {
                "distribution": "int_uniform", 
                "min": 5, 
                "max": 35
            },
            "min_samples_leaf": {
                "distribution": "int_uniform", 
                "min": 1, 
                "max": 20
            },
            "min_samples_split": {
                "distribution": "int_uniform", 
                "min": 2, 
                "max": 18
            },
            "criterion": {
                "values": ["gini", "entropy"]
            },
            "max_features": {
                "values": ["sqrt", "log2", None]
            },
            "umbral": {
                "distribution": "uniform", 
                "min": 0.3, 
                "max": 0.6
            },
            "class_weight": {
                "values": ["balanced", None]
            }
        }

    if metrica == "f2":
        metric_name = "val/f2_mean_cv"
    else:
        metric_name = "val/f1_mean_cv"

    sweep_config = {
        "name": f"DecisionTree-{metodo_elegido}-{metrica}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": "maximize"},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    metodo = input("\n Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros:" )
    metrica = input("\n Selecciona la métrica que quieres optimizar (f1/f2):" )
    clasificacion(metodo, metrica)