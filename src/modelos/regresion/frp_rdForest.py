import os
import sys
from pathlib import Path

import wandb
import yaml
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_regresion as train
from modelos.utils.metricas import evaluar_regresion

# Importar el módulo de anomalías (ajusta la ruta relativa según tu estructura exacta)
import modelos.utils.anomalias as anomalias
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers


WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "rdForest-frp-sweeps"
SWEEP_PATH = Path(__file__).with_name("sweep.yaml")
SEED = 42
NUM_IT = 0


def arboles_decision_regresion(X_train, X_val, X_test, y_train, y_val, y_test, nombre = None):
    global NUM_IT
    NUM_IT += 1
    run = wf.wandb_init(nombre=nombre, it=NUM_IT)
    config = wandb.config

    model = RandomForestRegressor(
        max_depth=config.max_depth,
        criterion=config.criterion,
        n_estimators=config.n_estimators,
        min_samples_leaf=config.min_samples_leaf,
        min_samples_split=config.min_samples_split,
        random_state=SEED,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)

    y_pred_train = model.predict(X_train)
    metricas_train = evaluar_regresion(
        y_train, y_pred_train, "Entrenamiento — Random Forest", en_log=True
    )

    y_pred_val = model.predict(X_val)
    metricas_val = evaluar_regresion(
        y_val, y_pred_val, "Validación — Random Forest", en_log=True
    )

    X_train = np.vstack((X_train, X_val))
    y_train = np.concatenate((y_train, y_val))
    model.fit(X_train, y_train)
    y_pred_test = model.predict(X_test)
    metricas_test = evaluar_regresion(
        y_test, y_pred_test, "Test — Random Forest", en_log=True
    )

    wandb.log({
        "rmse_test": metricas_test["rmse"],
        "overfitting_gap_rmse": metricas_val["rmse"] - metricas_train["rmse"],
        "train/rmse": metricas_train["rmse"],
        "train/mae": metricas_train["mae"],
        "train/r2": metricas_train["r2"],
        "train/rmse_mw": metricas_train.get("rmse_mw", 0),
        "val/rmse": metricas_val["rmse"],
        "val/mae": metricas_val["mae"],
        "val/r2": metricas_val["r2"],
        "val/rmse_mw": metricas_val.get("rmse_mw", 0),
        "test/rmse": metricas_test["rmse"],
        "test/mae": metricas_test["mae"],
        "test/r2": metricas_test["r2"],
        "test/rmse_mw": metricas_test.get("rmse_mw", 0),
        "n_features": X_train.shape[1],
    })

    wandb.sklearn.plot_regressor(
        model,
        X_train,
        X_test,
        y_train,
        y_test,
        model_name="RandomForestRegressor"
    )

    wandb.sklearn.plot_residuals(model, X_test.values, y_test.values)

    run.finish()

def main():
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cg.cargar_dataset_incendios(eliminar_correladas=False)
    
    X_train, X_val, X_test, y_train, y_val, y_test = train(X, y)

    nombre,iters = pers.pregunta_iters_nombre()

    def entrenamiento():
        arboles_decision_regresion(X_train, X_val, X_test, y_train, y_val, y_test, nombre)

    sweep_id = wf.crear_sweep_id(WANDB_PROJECT, SWEEP_PATH)
    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    main()