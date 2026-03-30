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

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SWEEP_PATH = Path(__file__).with_name("m_xgboost.yaml")
SEED = 42
NUM_IT = 0

def evaluacion_final(config, X_train_full, X_test, y_train_full, y_test, metodo):

    run = wandb.init(project=WANDB_PROJECT, entity=WANDB_ENTITY, tags=["Evaluacion Final", metodo])
    
    ratio = calcular_ratio_clases(y_train_full)
    umbral = config.get("umbral", 0.5)

    clf = xgb.XGBClassifier(
        n_estimators=int(config.get("n_estimators", 1000)), 
        learning_rate=config.get("learning_rate"),
        max_depth=config.get("max_depth"),
        subsample=config.get("subsample"),
        colsample_bytree=config.get("colsample_bytree"),
        scale_pos_weight=ratio,
        random_state=SEED,
        eval_metric="logloss",
        n_jobs=-1,
    )
    
    clf.fit(X_train_full, y_train_full)

    y_prob_test = clf.predict_proba(X_test)
    y_pred_test = (y_prob_test[:, 1] >= umbral).astype(int)

    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — XGBoost")

    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0)
    })

    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre = None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    # Hago esto para ver si le puedo dar más peso a la clase minoritaria (Incendios)

    ratio = calcular_ratio_clases(y_train_full)

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    f2_cv_scores, f1_cv_scores, recall_cv_scores = [], [], []
    f2_cv_scores_train, f1_cv_scores_train= [], []
    best_iterations = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        clf = xgb.XGBClassifier(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            scale_pos_weight=ratio,
            random_state=SEED,
            eval_metric="logloss",
            early_stopping_rounds=50, 
            n_jobs=-1,
        )
        
        # Modifico el clf.fit para que el entrenamiento se detenga si no mejora en 50 árboels (a ver si se evita así el overfitting)

        clf.fit(
            X_fold_train, y_fold_train,
            eval_set=[(X_fold_val, y_fold_val)],
            verbose=False
        )

        # Añado la mejor iteración automáticamente
        best_iterations.append(clf.best_iteration)

        # Métricas de validation
        y_val_prob = clf.predict_proba(X_fold_val)[:, 1]
        y_fold_pred = (y_val_prob >= config.umbral).astype(int)
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))

        y_train_prob = clf.predict_proba(X_fold_train)[:, 1]
        y_fold_pred_train = (y_train_prob >= config.umbral).astype(int)
        f1_cv_scores_train.append(f1_score(y_fold_train, y_fold_pred_train, zero_division=0))
        f2_cv_scores_train.append(fbeta_score(y_fold_train, y_fold_pred_train, beta=2, zero_division=0))
        

    wandb.log({
        "train/f1_mean_cv": float(np.mean(f1_cv_scores_train)),
        "train/f2_mean_cv": float(np.mean(f2_cv_scores_train)),
        "val/f1_mean_cv": float(np.mean(f1_cv_scores)),
        "val/f2_mean_cv": float(np.mean(f2_cv_scores)),
        "val/f1_std_cv": float(np.std(f1_cv_scores)), # Para ver si el modelo es estable
        "val/recall_mean_cv": float(np.mean(recall_cv_scores)),
        "diff/f1_overfit": float(np.mean(f1_cv_scores_train) - np.mean(f1_cv_scores)), # Control de memorización
        "best_iteration_mean": float(np.mean(best_iterations)),
        "scale_pos_weight": ratio
    })

    run.finish()

    # Me he cargado los gráficos pq no tenía mucho sentido mirar los gráficos del entrenamiento, en entrenamiento solo queremos buscar los hiperparámetros



def inicializar():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cargar_dataset_general(eliminar_correladas=False)
    X, y = pers.pregunta_PCA()
    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test

def calcular_ratio_clases(y):
    counts = y.value_counts()
    return counts[0] / counts[1]

def obtener_mejor_config(sweep_id, metrica_objetivo):

    """
    Función para obtener la mejor configuración de un sweep usando la métrica objetivo como filtro
    """

    api = wandb.Api()
    sweep = api.sweep(f"{WANDB_ENTITY}/{WANDB_PROJECT}/{sweep_id}")
    
    best_run = sorted(
        sweep.runs,
        key=lambda run: run.summary.get(metrica_objetivo, 0.0),
        reverse=True
    )[0]
    
    print(f"\n--- Mejor modelo encontrado: {best_run.name} ---")
    print(f"Métrica {metrica_objetivo}: {best_run.summary.get(metrica_objetivo):.4f}")
    
    return best_run.config

def clasificacion(metodo_elegido, metrica_elegida):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    if X_train_full is None: return
    
    iters, nombre = pers.pregunta_iters_nombre()
    
    if metodo_elegido == "grid":
        params = {
            "n_estimators": {"values": [1000, 2000, 5000]},
            "learning_rate": {"values": [0.01, 0.05, 0.1, 0.2]},
            "max_depth": {"values": [3, 4, 6, 9]},
            "subsample": {"values": [0.6, 0.8, 1.0]},
            "colsample_bytree": {"values": [0.5, 0.7, 1.0]},
            "umbral": {"values": [0.1, 0.2, 0.3, 0.4, 0.5]},
        }
    else: 
        params = {
            "n_estimators": {"values": [2000, 5000]},
            "learning_rate": {"distribution": "uniform", "min": 0.01, "max": 0.2},
            "max_depth": {"values": [3, 4, 5, 6, 8]},
            "subsample": {"distribution": "uniform", "min": 0.6, "max": 1.0},
            "colsample_bytree": {"distribution": "uniform", "min": 0.5, "max": 1.0},
            "umbral": {"distribution": "uniform", "min": 0.05, "max": 0.5},
        }

    metrica_limpia = metrica_elegida.lower().strip()
    metric_name = "val/f2_mean_cv" if "f2" in metrica_limpia else "val/f1_mean_cv"

    sweep_config = {
        "name": f"XGBoost-{metodo_elegido}-{metrica_elegida}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": "maximize"},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    
    wandb.agent(
        sweep_id=sweep_id,
        function=lambda: entrenamiento(X_train_full, y_train_full, nombre),
        count=iters
    )

    mejor_config = obtener_mejor_config(sweep_id, metric_name)
    evaluacion_final(mejor_config, X_train_full, X_test, y_train_full, y_test, metodo_elegido)

if __name__ == "__main__":
    metodo = input("\n Selecciona el metodo (grid o random) para la búsqueda de hiperparámetros:" )
    metrica = input("\n Selecciona la métrica que quieres optimizar (f1/f2):" )
    clasificacion(metodo, metrica)