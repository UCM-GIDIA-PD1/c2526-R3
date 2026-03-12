import os
import sys
from pathlib import Path

import wandb
import yaml
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import fbeta_score

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_estratificado
from modelos.utils.metricas import evaluar_clasificacion

load_dotenv()
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "arboles-decision-sweeps"
SWEEP_PATH = Path(__file__).with_name("randomforest_sweep.yaml")
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
    sweep_id = os.getenv("WANDB_SWEEP_ID") or os.getenv("SWEEP_ID")
    if sweep_id:
        print(f"Usando sweep existente: {sweep_id}")
        return sweep_id

    config_sweep = cargar_configuracion()
    sweep_id = wandb.sweep(config_sweep, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    print(f"Sweep creado desde YAML: {sweep_id}")
    return sweep_id


def wandb_init():
    return wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )


def arboles_decision(X_train, X_val, X_test, y_train, y_val, y_test):
    run = wandb_init()
    config = wandb.config

    model = RandomForestClassifier(
        max_depth=config.max_depth,
        criterion=config.criterion,
        n_estimators=config.n_estimators,
        class_weight=config.class_weight,
        min_samples_leaf=config.min_samples_leaf,
        min_samples_split=config.min_samples_split,
        random_state=SEED,
        n_jobs=-1,
    )

    model.fit(X_train, y_train)

    y_pred_val = model.predict(X_val)
    y_prob_val = model.predict_proba(X_val)[:, 1]
    metricas_val = evaluar_clasificacion(
        y_val, y_pred_val, y_prob_val, "Validación — Random Forest"
    )
    f1_5_val = fbeta_score(y_val, y_pred_val, beta=1.5, zero_division=0)

    y_pred_test = model.predict(X_test)
    y_prob_test = model.predict_proba(X_test)[:, 1]
    metricas_test = evaluar_clasificacion(
        y_test, y_pred_test, y_prob_test, "Test — Random Forest"
    )
    f1_5_test = fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)

    wandb.log({
        "f1_5_score": f1_5_val,
        "val/f1_5": f1_5_val,
        "val/f1": metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall": metricas_val["recall"],
        "val/accuracy": metricas_val["accuracy"],
        "val/roc_auc": metricas_val.get("roc_auc", 0),
        "test/f1_5": f1_5_test,
        "test/f1": metricas_test["f1"],
        "test/precision": metricas_test["precision"],
        "test/recall": metricas_test["recall"],
        "test/accuracy": metricas_test["accuracy"],
        "test/roc_auc": metricas_test.get("roc_auc", 0),
        "split": "estratificado",
        "eliminar_correladas": False,
        "n_features": X_train.shape[1],
    })

    wandb.sklearn.plot_classifier(
        model,
        X_train,
        X_val,
        y_train,
        y_val,
        y_pred_val,
        model.predict_proba(X_val),
        labels=["no_incendio", "incendio"],
        model_name="RandomForest",
        feature_names=X_train.columns.tolist(),
    )

    run.finish()


def main():
    # Todas las variables: no eliminar correladas por ahora
    X, y = cargar_dataset_general(eliminar_correladas=False)
    X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

    def entrenamiento():
        arboles_decision(X_train, X_val, X_test, y_train, y_val, y_test)

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
