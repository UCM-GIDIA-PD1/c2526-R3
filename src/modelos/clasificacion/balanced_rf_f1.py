import os
import sys
from pathlib import Path

import wandb
import yaml
import pandas as pd
from dotenv import load_dotenv
from imblearn.ensemble import BalancedRandomForestClassifier
from sklearn.metrics import f1_score

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_estratificado
from modelos.utils.metricas import evaluar_clasificacion


# Configuración inicial
load_dotenv()
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "balancedRandomForestClassifier"
SWEEP_PATH = Path(__file__).with_name("balanced_random_forest.yaml")
SEED = 42

# Cargar configuración del sweep
def cargar_configuracion(ruta_yaml: Path = SWEEP_PATH):
    with open(ruta_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def obtener_sweep_id():
    sweep_id = os.getenv("WANDB_SWEEP_ID") or os.getenv("SWEEP_ID")
    if sweep_id:
        print(f"Usando sweep existente: {sweep_id}")
        return sweep_id

    config_sweep = cargar_configuracion()
    sweep_id = wandb.sweep(config_sweep, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    print(f"Sweep creado desde YAML: {sweep_id}")
    return sweep_id

# Entrenamiento y evaluación
def arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test):
    run = wandb.init(entity=WANDB_ENTITY, project=WANDB_PROJECT)
    config = wandb.config

    # 1. Modelo con parámetros del Sweep
    model = BalancedRandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        sampling_strategy='auto',
        replacement=False,
        random_state=SEED,
        n_jobs=-1,
    )

    # 2. Validación
    model.fit(X_train, y_train)
    y_pred_val = model.predict(X_val)
    y_probas_val_full = model.predict_proba(X_val)
    y_prob_val_positivo = y_probas_val_full[:, 1]

    metricas_val = evaluar_clasificacion(y_val, y_pred_val, y_prob_val_positivo, "Validación")
    f1_val = f1_score(y_val, y_pred_val, zero_division=0)

    # 3. Re-entrenamiento con train + validation
    X_train_full = pd.concat([X_train, X_val])
    y_train_full = pd.concat([y_train, y_val])
    model.fit(X_train_full, y_train_full)

    # 4. Test Final
    y_pred_test = model.predict(X_test)
    y_probas_test_full = model.predict_proba(X_test)
    y_prob_test_positivo = y_probas_test_full[:, 1]

    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test_positivo, "Test Final")
    f1_test = f1_score(y_test, y_pred_test, zero_division=0)

    # 5. Log de métricas en WandB
    wandb.log({
        "f1_score_test": f1_test,  # Métrica objetivo
        "val/f1": f1_val,
        "test/f1": f1_test,
        "test/recall": metricas_test["recall"],
        "test/precision": metricas_test["precision"],
    })

    # 6. Reporte visual de sklearn
    wandb.sklearn.plot_classifier(
        model,
        X_train_full,
        X_test,
        y_train_full,
        y_test,
        y_pred_test,
        y_probas_test_full,
        labels=["no_incendio", "incendio"],
        model_name="BalancedRandomForest",
        feature_names=X_train_full.columns.tolist()
    )

    run.finish()

# Función principal
def clasificacion():
    X, y = cg.cargar_dataset_general(eliminar_correladas=False)
    X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

    def entrenamiento():
        arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test)

    sweep_id = obtener_sweep_id()
    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=25,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

if __name__ == "__main__":
    clasificacion()
