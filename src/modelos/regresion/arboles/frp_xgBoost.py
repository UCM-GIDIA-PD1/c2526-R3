import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

import wandb
import xgboost as xgb
from xgboost import callback
from wandb.sklearn import (
    plot_residuals,
    plot_feature_importances,
)

import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_regresion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGBoost-regresion"
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
        "method": metodo,
        "parameters": {
            "max_depth": {"values": [hiperparametros["max_depth"]]},
            "n_estimators": {"values": [hiperparametros["n_estimators"]]},
            "learning_rate": {"values": [hiperparametros["learning_rate"]]},
            "subsample": {"values": [hiperparametros["subsample"]]},
            "colsample_bytree": {"values": [hiperparametros["colsample_bytree"]]},
            "min_child_weight": {"values": [hiperparametros.get("min_child_weight", 1)]},
            "gamma": {"values": [hiperparametros.get("gamma", 0)]},
            "reg_alpha": {"values": [hiperparametros["reg_alpha"]]},
            "reg_lambda": {"values": [hiperparametros["reg_lambda"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    model = xgb.XGBRegressor(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            min_child_weight=config.min_child_weight,
            gamma=config.gamma,
            reg_alpha = config.reg_alpha,
            reg_lambda = config.reg_lambda,
            random_state=SEED,
            eval_metric="rmse",
            early_stopping_rounds=50,
            n_jobs=-1,
        )

    model.fit(
        X_train_full, y_train_full,
        eval_set=[(X_test, y_test)], 
        verbose=False                      
    )

    y_pred_test = model.predict(X_test)

    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — XGBoost Regressor")

    wandb.log({
        "test/rmse": float(metricas_test["rmse"]),
        "test/mae": float(metricas_test["mae"]),
        "test/r2": float(metricas_test["r2"]),
        "test/rmse_mw": float(metricas_test.get("rmse_mw", 0))
    })

    plot_residuals(model, X_train_full, y_train_full)
    plot_feature_importances(model)

    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    rmse_cv_scores, mae_cv_scores, r2_cv_scores = [], [], []
    rmse_cv_scores_train, mae_cv_scores_train, r2_cv_scores_train = [], [], []
    best_iterations = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        model = xgb.XGBRegressor(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            min_child_weight=config.min_child_weight,
            gamma=config.gamma,
            reg_alpha = config.reg_alpha,
            reg_lambda = config.reg_lambda,
            random_state=SEED,
            eval_metric="rmse",
            early_stopping_rounds=50,
            n_jobs=-1,
        )

        model.fit(
            X_fold_train, y_fold_train,
            eval_set=[(X_fold_val, y_fold_val)], 
            verbose=False                      
        )

        best_iterations.append(model.best_iteration)

        y_val_pred = model.predict(X_fold_val)
        rmse_cv_scores.append(np.sqrt(mean_squared_error(y_fold_val, y_val_pred)))
        mae_cv_scores.append(mean_absolute_error(y_fold_val, y_val_pred))
        r2_cv_scores.append(r2_score(y_fold_val, y_val_pred))

        y_train_pred = model.predict(X_fold_train)
        rmse_cv_scores_train.append(np.sqrt(mean_squared_error(y_fold_train, y_train_pred)))
        mae_cv_scores_train.append(mean_absolute_error(y_fold_train, y_train_pred))
        r2_cv_scores_train.append(r2_score(y_fold_train, y_train_pred))

    avg_best_trees = int(np.mean(best_iterations)) + 1

    wandb.log({
        "train/rmse_mean_cv": float(np.mean(rmse_cv_scores_train)),
        "train/mae_mean_cv": float(np.mean(mae_cv_scores_train)),
        "train/r2_mean_cv": float(np.mean(r2_cv_scores_train)),
        "val/rmse_mean_cv": float(np.mean(rmse_cv_scores)),
        "val/mae_mean_cv": float(np.mean(mae_cv_scores)),
        "val/r2_mean_cv": float(np.mean(r2_cv_scores)),
        "overfitting_gap_rmse_cv": float(np.mean(rmse_cv_scores) - np.mean(rmse_cv_scores_train))
    })

    model = xgb.XGBRegressor(
        n_estimators=avg_best_trees,
        learning_rate=config.learning_rate,
        max_depth=config.max_depth,
        subsample=config.subsample,
        colsample_bytree=config.colsample_bytree,
        min_child_weight=config.min_child_weight,
        gamma=config.gamma,
        reg_alpha=config.reg_alpha,
        reg_lambda=config.reg_lambda,
        random_state=SEED,
        n_jobs=-1,
        eval_metric="rmse"
    )

    model.fit(X_train_full, y_train_full)
    plot_feature_importances(model)
    plot_residuals(model, X_train_full, y_train_full)

    run.finish()


def inicializar():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cg.cargar_dataset_frp()
    X, y = pers.pregunta_PCA()
    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test


def regresion(metodo_elegido, metrica_elegida):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    iters, nombre = pers.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train_full, y_train_full, nombre)

    if metodo_elegido == "grid":
        params = {
            "n_estimators": {"values": [100, 500, 1000, 2000]},
            "learning_rate": {"values": [0.01, 0.05, 0.1, 0.2]},
            "max_depth": {"values": [3, 6]},
            "subsample": {"values": [0.6, 0.8, 1.0]},
            "colsample_bytree": {"values": [0.5, 0.7, 1.0]},
            "min_child_weight": {"values": [5, 10, 15, 20]},
            "gamma": {"values": [0, 0.1, 0.3, 0.5, 0.7]},
            "reg_alpha": {"values": [0, 0.5, 0.9]},
            "reg_lambda": {"values": [0.5, 1, 3, 5]}
        }
    else: 
        params = {
            "n_estimators": {"values": [100, 500, 1000, 2000]},
            "learning_rate": {"distribution": "uniform", "min": 0.01, "max": 0.2},
            "max_depth": {"values": [3, 6]},
            "subsample": {"distribution": "uniform", "min": 0.6, "max": 1.0},
            "colsample_bytree": {"distribution": "uniform", "min": 0.5, "max": 1.0},
            "min_child_weight": {"distribution": "uniform", "min": 5, "max": 20},
            "gamma": {"distribution": "uniform", "min": 0, "max": 0.7},
            "reg_alpha": {"distribution": "uniform", "min": 0, "max": 1},
            "reg_lambda": {"distribution": "uniform", "min": 0.5, "max": 5}
        }

    metrica_limpia = metrica_elegida.lower().strip()
    
    if metrica_limpia in ["rmse", "mae"]:
        metric_name = f"val/{metrica_limpia}_mean_cv"
        metric_goal = "minimize"
    elif metrica_limpia == "r2":
        metric_name = "val/r2_mean_cv"
        metric_goal = "maximize"
    else:
        print("metrica no reconocida, se elige el RMSE por defecto")
        metric_name = "val/rmse_mean_cv"
        metric_goal = "minimize"

    sweep_config = {
        "name": f"XGBoost-Regresion-{metodo_elegido}-{metrica_limpia}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": metric_goal},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    
    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iters
    )

if __name__ == "__main__":
    metodo = input("\n Selecciona el método (grid o random) para la búsqueda de hiperparámetros: " )
    metrica = input("\n Selecciona la métrica que quieres optimizar (rmse/mae/r2): " )
    regresion(metodo, metrica)