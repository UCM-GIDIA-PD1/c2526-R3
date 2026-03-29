import numpy as np
from pathlib import Path

import wandb
import yaml
import pandas as pd
from dotenv import load_dotenv
from imblearn.ensemble import BalancedRandomForestClassifier
from sklearn.metrics import fbeta_score, recall_score

import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion

import modelos.utils.personalizacion as pers
import modelos.utils.wandbFunctions as wf


# Configuración inicial

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "balancedRandomForestClassifier"
SWEEP_PATH = Path(__file__).with_name("balanced_random_forest.yaml")
SEED = 42
NUM_IT = 0

TIPO_CV = 'estratificado'

# Entrenamiento y evaluación
def arboles_decision_clasificacion(X_train_full, X_test, y_train_full, y_test, detallados=False, nombre = None):
    
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

    # 2. Validación Cruzada
    cv_generator = generador_cv(tipo_cv=TIPO_CV, n_splits=4, seed=SEED)
    f2_val_scores = []
    recall_val_scores = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        # Entrenar n - 1 grupos y predecimos el otro
        model.fit(X_fold_train, y_fold_train)
        y_fold_pred = model.predict(X_fold_val)
        
        # Guardamos los valores de f2-score y del recall 
        f2_fold = fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0)
        recall_fold = recall_score(y_fold_val, y_fold_pred, zero_division=0)
        f2_val_scores.append(f2_fold)
        recall_val_scores.append(recall_fold)

    # Creamos el valor medio para saber la efectividad
    f2_val_mean = np.mean(f2_val_scores)
    recall_val_mean = np.mean(recall_val_scores)

    # 3. Entrenamos el modelo final con el 80%
    model.fit(X_train_full, y_train_full)

    # 4. Test Final
    y_pred_test = model.predict(X_test)
    y_probas_test_full = model.predict_proba(X_test)
    y_prob_test_positivo = y_probas_test_full[:, 1]

    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test_positivo, "Test Final")
    f2_test = fbeta_score(y_test, y_pred_test, beta=2, zero_division=0)

    # 5. Log de métricas en WandB
    wandb.log({
        "val/f2_mean_cv": f2_val_mean,
        "val/recall_mean_cv": recall_val_mean,
        "test/f2_score": f2_test,
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

    wf.matriz_confusion_feature_importance(model, y_pred_test, y_test, X_train_full.columns.tolist())
    
    run.finish()

# Función principal
def clasificacion():

    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = pers.pregunta_PCA()

    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y)

    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    detallado = input("¿Quieres ver los resultados detallados de cada iteración (curvas AUC, ROC, etc.)? (s/n) : ")
    detallados = False

    if detallado.lower() == "s":
        detallados = True
        print("Se mostrarán los resultados detallados de cada iteración.")
    else:
        print("No se mostrarán los resultados detallados de cada iteración.")


    def entrenamiento():
        arboles_decision_clasificacion(X_train_full, X_test, y_train_full, y_test, detallados, nombre)

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