import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.ensemble import RandomForestRegressor
import matplotlib

matplotlib.use('Agg')

import wandb
from wandb.sklearn import plot_residuals, plot_feature_importances, plot_learning_curve, plot_summary_metrics
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_regresion
import modelos.utils.carga_datos as cg
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as per

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "rdForest-frp-sweeps"
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
        "method": metodo,
        "parameters": {
            "max_depth": {"values": [hiperparametros["max_depth"]]},
            "n_estimators": {"values": [hiperparametros["n_estimators"]]},
            "min_samples_split": {"values": [hiperparametros["min_samples_split"]]},
            "min_samples_leaf": {"values": [hiperparametros["min_samples_leaf"]]},
            "criterion": {"values": [hiperparametros["criterion"]]},
            "max_features": {"values": [hiperparametros["max_features"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    model = RandomForestRegressor(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    model.fit(X_train_full, y_train_full)

    y_pred_test = model.predict(X_test)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — RandomForest Regressor", en_log=True)

    wandb.log({
        "test/rmse": float(metricas_test["rmse"]),
        "test/mae": float(metricas_test["mae"]),
        "test/r2": float(metricas_test["r2"])
    })

    try:
        plot_residuals(model, X_test.values, y_test.values)
        plot_feature_importances(model)
        plot_learning_curve(model, X_train_full.values, y_train_full.values)
        plot_summary_metrics(model, X_test.values, y_test.values)
    except Exception as e:
        print(f"error al generar las graficas de wandb: {e}")

    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    model = RandomForestRegressor(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    rmse_cv_scores, mae_cv_scores, r2_cv_scores = [], [], []
    rmse_cv_train, mae_cv_train, r2_cv_train = [], [], []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train, X_fold_val = X_train_full.iloc[train_idx], X_train_full.iloc[val_idx]
        y_fold_train, y_fold_val = y_train_full.iloc[train_idx], y_train_full.iloc[val_idx]

        model.fit(X_fold_train, y_fold_train)
        
        y_val_pred = model.predict(X_fold_val)
        rmse_cv_scores.append(np.sqrt(mean_squared_error(y_fold_val, y_val_pred)))
        mae_cv_scores.append(mean_absolute_error(y_fold_val, y_val_pred))
        r2_cv_scores.append(r2_score(y_fold_val, y_val_pred))
    
        y_t_pred = model.predict(X_fold_train)
        rmse_cv_train.append(np.sqrt(mean_squared_error(y_fold_train, y_t_pred)))
        mae_cv_train.append(mean_absolute_error(y_fold_train, y_t_pred))
        r2_cv_train.append(r2_score(y_fold_train, y_t_pred))

    wandb.log({
        "train/rmse_mean_cv": np.mean(rmse_cv_train),
        "train/mae_mean_cv": np.mean(mae_cv_train),
        "train/r2_mean_cv": np.mean(r2_cv_train),
        "val/rmse_mean_cv": np.mean(rmse_cv_scores), 
        "val/mae_mean_cv": np.mean(mae_cv_scores),
        "val/r2_mean_cv": np.mean(r2_cv_scores),
        "overfitting_gap_rmse": np.mean(rmse_cv_scores) - np.mean(rmse_cv_train)
    })

    model.fit(X_train_full, y_train_full)
    
    try:
        plot_feature_importances(model)
    except Exception as e:
        pass

    run.finish()


def inicializar():
    if not wf.inicializar_apikey_wandb():
        return None, None, None, None
    
    # X, y = cg.cargar_dataset_frp()
    
    X, y = per.pregunta_PCA(False) 
    X_train, X_test, y_train, y_test = split_temporal(X, y, date_col='date', test_size=0.2)
    X_train, X_test = per.anomalias(X_train, X_test)

    return X_train, X_test, y_train, y_test


def regresion(metodo_elegido, metrica_elegida):
    X_train, X_test, y_train, y_test = inicializar()

    if X_train is None:
        return
  
    iters, nombre = per.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train, y_train, nombre)

    if metodo_elegido == "grid":
        params = {
            "n_estimators": {"values": [100, 300, 500, 900]},
            "max_depth": {"values": [5, 15, 25, 35]},
            "min_samples_leaf": {"values": [1, 5, 10]},
            "min_samples_split": {"values": [2, 10, 18]},
            "criterion": {"values": ["squared_error", "absolute_error", "friedman_mse"]},
            "max_features": {"values": ["sqrt", "log2", None]}
        }
    elif metodo_elegido == 'bayes': 
        params = {
            "n_estimators": {
                "distribution": "int_uniform", 
                "min": 100, 
                "max": 1000
            },
            "max_depth": {
                "distribution": "int_uniform",
                "min": 3, 
                "max": 20
            },
            "min_samples_leaf": {
                "distribution": "int_uniform", 
                "min": 1, 
                "max": 15 
            },
            "min_samples_split": {
                "distribution": "int_uniform", 
                "min": 2, 
                "max": 30
            },
            "criterion": {
                "values": ["squared_error"]
            },
            "max_features": {
                "values": ["sqrt", "log2", 0.5, 0.8] 
            }
        }
    else:
        params = {
            "n_estimators": {
                "values": [100, 200, 300, 400, 500, 600, 700, 800, 900]
            },
            "max_depth": {
                "distribution": "int_uniform", 
                "min": 5, 
                "max": 35
            },
            "min_samples_leaf": {
                "distribution": "int_uniform", 
                "min": 1, 
                "max": 10
            },
            "min_samples_split": {
                "distribution": "int_uniform", 
                "min": 2, 
                "max": 18
            },
            "criterion": {
                "values": ["squared_error", "absolute_error"]
            },
            "max_features": {
                "values": ["sqrt", "log2", None] 
            }
        }


    metrica_limpia = metrica_elegida.lower().strip()
    if "rmse" in metrica_limpia:
        metric_name = "val/rmse_mean_cv"
        goal = "minimize"
    elif "mae" in metrica_limpia:
        metric_name = "val/mae_mean_cv"
        goal = "minimize"
    elif "r2" in metrica_limpia:
        metric_name = "val/r2_mean_cv"
        goal = "maximize"
    else:
        print("Metrica no reconocida, se usara RMSE por defecto")
        metric_name = "val/rmse_mean_cv"
        goal = "minimize"

    sweep_config = {
        "name": f"RF-Reg-{metodo_elegido}-{metrica_elegida}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": goal},
        "parameters": params
    }
    
    sweep_id = wandb.sweep(
        sweep_config, 
        entity=WANDB_ENTITY, 
        project=WANDB_PROJECT
    )

    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iters
    )

if __name__ == "__main__":
    metodo = input("\n Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros: ")
    metrica = input("\n Selecciona la métrica que quieres optimizar (rmse/mae/r2): ")
    regresion(metodo, metrica)