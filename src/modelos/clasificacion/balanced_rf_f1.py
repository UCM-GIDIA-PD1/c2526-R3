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

import modelos.utils.personalizacion as pers
import modelos.utils.wandbFunctions as wf


# Configuración inicial

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "balancedRandomForestClassifier"
SWEEP_PATH = Path(__file__).with_name("balanced_random_forest.yaml")
SEED = 42
NUM_IT = 0

# Entrenamiento y evaluación
def arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test, detallados=False, nombre = None):
    
    global NUM_IT

    NUM_IT += 1
    run = wf.wandb_init(nombre,NUM_IT)
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
    # y_probas_val_full = model.predict_proba(X_val)
    # y_prob_val_positivo = y_probas_val_full[:, 1]

    # metricas_val = evaluar_clasificacion(y_val, y_pred_val, y_prob_val_positivo, "Validación")
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

    if detallados:
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

    wf.matriz_confusion_feature_importance(model, y_pred_val, y_val, X_train.columns.tolist())
    
    run.finish()

# Función principal
def clasificacion():

    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = pers.pregunta_PCA()

    X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

    X_train, X_val, X_test = pers.anomalias(X_train, X_val, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    detallado = input("¿Quieres ver los resultados detallados de cada iteración (curvas AUC, ROC, etc.)? (s/n) : ")
    detallados = False

    if detallado.lower() == "s":
        detallados = True
        print("Se mostrarán los resultados detallados de cada iteración.")
    else:
        print("No se mostrarán los resultados detallados de cada iteración.")


    def entrenamiento():
        arboles_decision_clasificacion(X_train, X_val, X_test, y_train, y_val, y_test, detallados, nombre)

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
