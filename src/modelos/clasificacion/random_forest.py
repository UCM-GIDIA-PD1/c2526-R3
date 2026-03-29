import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import wandb
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import fbeta_score, recall_score, f1_score

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.personalizacion as pers
import modelos.utils.wandbFunctions as wf

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "sweep_random_forest_umbral_smote"
SWEEP_PATH = Path(__file__).with_name("randomforest_sweep.yaml")
SEED = 42
NUM_IT = 0
TIPO_CV = 'estratificado'

def arboles_decision_clasificacion(X_train_full, X_test, y_train_full, y_test, detallados=False, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    if config.max_features == "None":
        max_f = None
    else:
        max_f = config.max_features

    model = RandomForestClassifier(
        max_depth=config.max_depth,
        criterion=config.criterion,
        n_estimators=config.n_estimators,
        class_weight=config.class_weight,
        min_samples_leaf=config.min_samples_leaf,
        min_samples_split=config.min_samples_split,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    # 1. Validación Cruzada
    cv_generator = generador_cv(tipo_cv=TIPO_CV, n_splits=4, seed=SEED)
    f2_cv_scores = []
    f1_cv_scores = []
    recall_cv_scores = []
    f1_5_cv_scores = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        model.fit(X_fold_train, y_fold_train)
        
        y_fold_prob = model.predict_proba(X_fold_val)[:, 1]
        y_fold_pred = (y_fold_prob >= config.umbral).astype(int)

        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))
        f1_5_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=1.5, zero_division=0))

    f2_score_mean = np.mean(f2_cv_scores)
    f1_score_mean = np.mean(f1_cv_scores)
    recall_score_mean = np.mean(recall_cv_scores)
    f1_5_score_mean = np.mean(f1_5_cv_scores)

    # 2. Entrenamiento final con el full train
    model.fit(X_train_full, y_train_full)

    y_prob_train = model.predict_proba(X_train_full)[:, 1]
    y_pred_train = (y_prob_train >= config.umbral).astype(int)
    metricas_train = evaluar_clasificacion(y_train_full, y_pred_train, y_prob_train, "Entrenamiento — RF")

    y_prob_test = model.predict_proba(X_test)[:, 1]
    y_pred_test = (y_prob_test >= config.umbral).astype(int)
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test, "Test — RF")

    f1_train_final = fbeta_score(y_train_full, y_pred_train, beta=1.5, zero_division=0)
    f1_test_final = fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)

    # 3. Loggeo de métricas
    log_dict = {
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
        "test/accuracy": metricas_test["accuracy"]
    }

    if detallados:
        log_dict["overfitting_gap_f1_5"] = f1_train_final - f1_5_score_mean
        wandb.log(log_dict)
        wandb.sklearn.plot_classifier(
            model, X_train_full, X_test, y_train_full, y_test, y_pred_test,
            model.predict_proba(X_test), labels=["no_incendio", "incendio"],
            model_name="RandomForest", feature_names=X_train_full.columns.tolist()
        )
    else:
        wandb.log(log_dict)
        wf.matriz_confusion_feature_importance(model, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()

def clasificacion():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cg.cargar_dataset_general()

    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y)

    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    detallado = input("¿Quieres ver los resultados detallados (s/n) : ")
    detallados = detallado.lower() == "s"

    def entrenamiento():
        arboles_decision_clasificacion(X_train_full, X_test, y_train_full, y_test, detallados, nombre)

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