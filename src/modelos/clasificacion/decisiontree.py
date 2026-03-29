import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import fbeta_score, recall_score, f1_score
import wandb
from pathlib import Path

import modelos.utils.wandbFunctions as wf
import modelos.utils.carga_datos as cg
import modelos.utils.personalizacion as pers
import modelos.utils.metricas as met
import modelos.utils.particiones as part

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "decision_tree_clasificacion"
NUM_IT = 0
SWEEP_PATH = Path(__file__).with_name("decisiontree_sweep.yaml")
TIPO_CV = 'estratificado'

def decision_tree(X_train_full, X_test, y_train_full, y_test, nombre):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    if config.max_features == "None":
        max_f = None
    else:
        max_f = config.max_features

    tree = DecisionTreeClassifier(
        max_depth=config.max_depth,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        class_weight=config.class_weight,
        random_state=42
    )
    
    cv_generator = part.generador_cv(tipo_cv=TIPO_CV, n_splits=4, seed=42)
    f2_cv_scores = []
    f1_cv_scores = []
    recall_cv_scores = []
    f1_5_cv_scores = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        tree.fit(X_fold_train, y_fold_train)
        
        y_fold_prob = tree.predict_proba(X_fold_val)[:, 1]
        y_fold_pred = (y_fold_prob >= config.umbral).astype(int)

        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))
        f1_5_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=1.5, zero_division=0))

    f2_score_mean = np.mean(f2_cv_scores)
    f1_score_mean = np.mean(f1_cv_scores)
    recall_score_mean = np.mean(recall_cv_scores)
    f1_5_score_mean = np.mean(f1_5_cv_scores)

    tree.fit(X_train_full, y_train_full)

    y_prob_train = tree.predict_proba(X_train_full)[:, 1]
    y_pred_train = (y_prob_train >= config.umbral).astype(int)
    metricas_train = met.evaluar_clasificacion(
        y_train_full, y_pred_train, y_prob_train, "Entrenamiento — Decision Tree"
    )

    y_prob_test = tree.predict_proba(X_test)[:, 1]
    y_pred_test = (y_prob_test >= config.umbral).astype(int)
    metricas_test = met.evaluar_clasificacion(
        y_test, y_pred_test, y_prob_test, "Test — Decision Tree"
    )

    f1_train_final = fbeta_score(y_train_full, y_pred_train, beta=1.5, zero_division=0)
    f1_test_final = fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)

    wandb.log({
        "f1_5_score": f1_5_score_mean,
        "val/f1_mean_cv": f1_score_mean,
        "val/f2_mean_cv": f2_score_mean,
        "val/recall_mean_cv": recall_score_mean,
        "val/f1_5_mean_cv": f1_5_score_mean,
        "train/f1_5": f1_train_final,
        "train/f1": metricas_train["f1"],
        "train/precision": metricas_train["precision"],
        "train/recall": metricas_train["recall"],
        "train/accuracy": metricas_train["accuracy"],
        "test/f1_5": f1_test_final,
        "test/f1": metricas_test["f1"],
        "test/precision": metricas_test["precision"],
        "test/recall": metricas_test["recall"],
        "test/accuracy": metricas_test["accuracy"],
    })

    wf.matriz_confusion_feature_importance(tree, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()

def clasificacion():

    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cg.cargar_dataset_general()

    X_train_full, X_test, y_train_full, y_test = part.split_temporal(X, y)

    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    def entrenamiento():
        decision_tree(X_train_full, X_test, y_train_full, y_test, nombre)

    sweep_id = wf.crear_sweep_id(WANDB_PROJECT, SWEEP_PATH)

    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

    print(f"Listo!! Ejecutadas {iters} iteraciones")

if __name__ == "__main__":
    clasificacion()