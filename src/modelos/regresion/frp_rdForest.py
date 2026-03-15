import os
import sys
from pathlib import Path

import wandb
import yaml
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_regresion as train
from modelos.utils.metricas import evaluar_regresion

# Importar el módulo de anomalías (ajusta la ruta relativa según tu estructura exacta)
import modelos.utils.anomalias as anomalias

load_dotenv()
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "rdForest-frp-sweeps"
SWEEP_PATH = Path(__file__).with_name("sweep.yaml")
SEED = 42

def cargar_configuracion(ruta_yaml: Path = SWEEP_PATH):
    """Carga la configuración del sweep desde el YAML."""
    with open(ruta_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def obtener_sweep_id():
    """
    Usa un sweep ya creado si existe en variables de entorno.
    Si no existe, crea uno nuevo a partir del YAML.
    """
    sweep_id = os.getenv("WANDB_SWEEP_ID") or os.getenv("SWEEP_ID_REGRESION")
    if sweep_id:
        print(f"Usando sweep existente: {sweep_id}")
        return sweep_id

    config_sweep = cargar_configuracion()
    sweep_id = wandb.sweep(config_sweep, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    print(f"Sweep creado desde YAML: {sweep_id}")
    return sweep_id


def wandb_init(nombre, it):
    return wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        # name = f'{nombre}-{it}'
    )

def arboles_decision_regresion(X_train, X_val, X_test, y_train, y_val, y_test):
    # global NUM_IT
    # NUM_IT += 1
    run = wandb_init(nombre=None, it=1)
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

    scores_anomalia = X_train['anomaly_ISOL_FOR'].values
    pesos_train = (scores_anomalia.max() - scores_anomalia) + 1

    X_train_fit = X_train.drop(columns=['anomaly_ISOL_FOR'])
    X_val_fit = X_val.drop(columns=['anomaly_ISOL_FOR'])
    X_test_fit = X_test.drop(columns=['anomaly_ISOL_FOR'])

    model.fit(X_train_fit, y_train, sample_weight=pesos_train)

    y_pred_train = model.predict(X_train_fit)
    metricas_train = evaluar_regresion(
        y_train, y_pred_train, "Entrenamiento — Random Forest", en_log=True
    )

    y_pred_val = model.predict(X_val_fit)
    metricas_val = evaluar_regresion(
        y_val, y_pred_val, "Validación — Random Forest", en_log=True
    )

    y_pred_test = model.predict(X_test_fit)
    metricas_test = evaluar_regresion(
        y_test, y_pred_test, "Test — Random Forest", en_log=True
    )

    wandb.log({
        "rmse_val": metricas_val["rmse"],
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
        "n_features": X_train_fit.shape[1],
    })

    wandb.sklearn.plot_regressor(
        model,
        X_train_fit,
        X_val_fit,
        y_train,
        y_val,
        model_name="RandomForestRegressor"
    )

    run.finish()

def main():
    X, y = cg.cargar_dataset_incendios(eliminar_correladas=True)
    
    X_train, X_val, X_test, y_train, y_val, y_test = train(X, y)

    X_train, X_val, X_test = anomalias.isolationForest(X_train, X_val, X_test)

    def entrenamiento():
        arboles_decision_regresion(X_train, X_val, X_test, y_train, y_val, y_test)

    sweep_id = obtener_sweep_id()
    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=25,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    main()