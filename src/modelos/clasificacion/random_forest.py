import os
import sys
from pathlib import Path

import wandb
import yaml
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import fbeta_score

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_estratificado
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.anomalias as anom
import numpy as np
from imblearn.over_sampling import SMOTE

load_dotenv()
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "sweep_random_forest_umbral_smote"
SWEEP_PATH = Path(__file__).with_name("randomforest_sweep.yaml")
SEED = 42
NUM_IT = 0


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


def wandb_init(nombre, it):
    return wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        name = f'{nombre}-{it}'
    )


def arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test, detallados=False, nombre = None):
    global NUM_IT
    X_train, X_val, X_test = anom.isolationForest(X_train,X_val,X_test)
    NUM_IT += 1
    run = wandb_init(nombre,NUM_IT)
    config = wandb.config

    if config.max_features == "None": # Daba error sino porque el YAML devuelve un string
        max_f = None
    else:
        max_f = config.max_features

    #smote = SMOTE(random_state=SEED, sampling_strategy= config.sampling_strategy)
    #X_train_2, y_train_2 = smote.fit_resample(X_train, y_train)
    
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

    model.fit(X_train, y_train)


    # ---------------- Entrenamientos por split y evaluaciones -----------------
    #y_pred_train = model.predict(X_train) estas son con el umbral por defecto de 0.5
    y_prob_train = model.predict_proba(X_train)[:, 1]
    y_pred_train = (y_prob_train >= config.umbral).astype(int) 
    metricas_train = evaluar_clasificacion(
        y_train, y_pred_train, y_prob_train, "Entrenamiento — Random Forest"
    )
    f1_5_train = fbeta_score(y_train, y_pred_train, beta=1.5, zero_division=0)

    #y_pred_val = model.predict(X_val)
    y_prob_val = model.predict_proba(X_val)[:, 1]
    y_pred_val = (y_prob_val >= config.umbral).astype(int)
    metricas_val = evaluar_clasificacion(
        y_val, y_pred_val, y_prob_val, "Validación — Random Forest"
    )
    f1_5_val = fbeta_score(y_val, y_pred_val, beta=1.5, zero_division=0)

    #y_pred_test = model.predict(X_test)
    y_prob_test = model.predict_proba(X_test)[:, 1]
    y_pred_test = (y_prob_test >= config.umbral).astype(int)
    metricas_test = evaluar_clasificacion(
        y_test, y_pred_test, y_prob_test, "Test — Random Forest"
    )
    f1_5_test = fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)

    if detallados:
        wandb.log({
        "f1_5_score": f1_5_val,
        
        "overfitting_gap_f1_5": f1_5_train - f1_5_val,

        "train/f1_5": f1_5_train,
        "train/f1": metricas_train["f1"],
        "train/precision": metricas_train["precision"],
        "train/recall": metricas_train["recall"],
        "train/accuracy": metricas_train["accuracy"],
        "train/roc_auc": metricas_train.get("roc_auc", 0),

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

    else: # Muestra solo las métricas principales sin curvas ni gráficos
        
        wandb.log({
        "f1_5_score": f1_5_val,

        "train/f1_5": f1_5_train,
        "train/f1": metricas_train["f1"],
        "train/precision": metricas_train["precision"],
        "train/recall": metricas_train["recall"],
        "train/accuracy": metricas_train["accuracy"],

        "val/f1_5": f1_5_val,
        "val/f1": metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall": metricas_val["recall"],
        "val/accuracy": metricas_val["accuracy"],

        })

        wandb.sklearn.plot_confusion_matrix(y_val, y_pred_val, labels=["no_incendio", "incendio"])
        
        wandb.sklearn.plot_feature_importances(model, feature_names=X_train.columns.tolist())

    run.finish()


def clasificacion():
    X, y = cg.cargar_dataset_general(eliminar_correladas=False)
    X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

    detallados = False

    iters = int(input("¿Cuántas iteraciones quieres ejecutar? : "))
    detallado = input("¿Quieres ver los resultados detallados de cada iteración (curvas AUC, ROC, etc.)? (s/n) : ")
    nombre = input("Por último, como quieres llamar a los runs de este sweep? ")

    if detallado.lower() == "s":
        detallados = True
        print("Se mostrarán los resultados detallados de cada iteración.")
    else:
        print("No se mostrarán los resultados detallados de cada iteración.")


    def entrenamiento():
        arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test, detallados, nombre)

    sweep_id = obtener_sweep_id()

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
