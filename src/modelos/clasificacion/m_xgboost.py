import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score
from sklearn.neighbors import BallTree
import matplotlib.pyplot as plt
from datetime import timedelta

import wandb
import xgboost as xgb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

from modelos.utils.carga_datos import cargar_dataset_general, cargar_dataset_general_con_tiempos
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers
import modelos.clasificacion.ventanas_temporales as ventana

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SWEEP_PATH = Path(__file__).with_name("m_xgboost.yaml")
SEED = 42
NUM_IT = 0

def evaluacion_final(config, X_train_full, X_test, y_train_full, y_test, metodo):

    run = wandb.init(
        project=WANDB_PROJECT, 
        entity=WANDB_ENTITY, 
        name="Mejor Modelo Test", 
        tags=["Evaluacion Final", metodo],
        reinit=True 
    )

    ratio = calcular_ratio_clases(y_train_full)
    

    clf = xgb.XGBClassifier(
        n_estimators=int(config.get("n_estimators", 1000)), 
        learning_rate=config.get("learning_rate"),
        max_depth=config.get("max_depth"),
        subsample=config.get("subsample"),
        colsample_bytree=config.get("colsample_bytree"),
        min_child_weight=config.get("min_child_weight", 1), 
        gamma=config.get("gamma", 0),
        scale_pos_weight=ratio,
        random_state=SEED,
        eval_metric="aucpr",
        n_jobs=-1,
    )
    
    y_prob_train = clf.predict_proba(X_train_full)[:, 1]
    umbral_optimo = ventana.encontrar_mejor_umbral(y_train_full, y_prob_train)
    print(f"\n Umbral óptimo calculado para el Test: {umbral_optimo:.2f}")

    y_prob_test = clf.predict_proba(X_test)
    y_pred_test = (y_prob_test[:, 1] >= umbral_optimo).astype(int)

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
            min_child_weight=config.get("min_child_weight", 1),
            gamma=config.get("gamma", 0),
            scale_pos_weight=ratio,
            random_state=SEED,
            eval_metric="aucpr",
            early_stopping_rounds=100, 
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
    
    X, y = cargar_dataset_general_con_tiempos(eliminar_correladas=False)

    df_completo = pd.concat([X, y.rename('incendio')], axis=1)
    df_completo['date'] = pd.to_datetime(df_completo['date'])
    
    df_features = ventana.menu_ventanas_temporales(df_completo)
    
    print("\n--- Post-procesando variables generadas (Logs y Hubo Incendio) ---")
    for col in df_features.columns:
        if col.startswith('incendios_recientes_'):
            w = col.split('_')[-1]  
            df_features[f'hubo_incendio_{w}'] = (df_features[col] > 0).astype(int)
            dias = int(w.replace('d',''))
            df_features[f'frecuencia_incendios_{w}'] = df_features[col] / dias
            df_features[f'log_{col}'] = np.log1p(df_features[col])
        elif col.startswith('dias_ultimo_incendio_'):
            df_features[f'log_{col}'] = np.log1p(df_features[col].clip(lower=0))
    
    if 'incendios_estacionales' in df_features.columns:
        df_features['hubo_incendio_estacional'] = (df_features['incendios_estacionales'] > 0).astype(int)
        df_features['log_incendios_estacional'] = np.log1p(df_features['incendios_estacionales'])
    
    if 'dias_ultimo_incendio_estacional' in df_features.columns:
        df_features['log_dias_estacional'] = np.log1p(df_features['dias_ultimo_incendio_estacional'].clip(lower=0))

    y_final = df_features['incendio']
    X_final = df_features.drop(['incendio', 'date'], axis=1, errors='ignore')

    df_para_limpieza = pd.concat([X_final, y_final.rename('incendio')], axis=1)
    X_final, y_final = pers.pregunta_PCA(df=df_para_limpieza)

    X_train_full, X_test, y_train_full, y_test = split_temporal(X_final, y_final, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test

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
    elif metodo_elegido == "random":
        params = {
            "n_estimators": {"values": [2000, 5000]},
            "learning_rate": {"distribution": "uniform", "min": 0.01, "max": 0.2},
            "max_depth": {"values": [3, 4, 5, 6, 8]},
            "subsample": {"distribution": "uniform", "min": 0.6, "max": 1.0},
            "colsample_bytree": {"distribution": "uniform", "min": 0.5, "max": 1.0},
            "umbral": {"distribution": "uniform", "min": 0.05, "max": 0.5},
        }
    else:
        params = {
        "n_estimators": {"values": [2000, 3000]}, 
        "learning_rate": {"distribution": "log_uniform_values", "min": 0.005, "max": 0.05}, 
        "max_depth": {"values": [5, 6, 8, 10]}, 
        "min_child_weight": {"distribution": "int_uniform", "min": 3, "max": 8},
        "gamma": {"distribution": "uniform", "min": 0.5, "max": 3.0},
        "subsample": {"distribution": "uniform", "min": 0.6, "max": 0.9},
        "colsample_bytree": {"distribution": "uniform", "min": 0.5, "max": 0.9},
        "umbral": {"distribution": "uniform", "min": 0.5, "max": 0.95} 
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
    metodo = input("\n Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros: ").strip().lower()
    metrica = input("\n Selecciona la métrica que quieres optimizar (f1/f2): ").strip().lower()
    clasificacion(metodo, metrica)