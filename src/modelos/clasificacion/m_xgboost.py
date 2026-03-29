import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score

import wandb
import xgboost as xgb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

# Configuración de rutas para importar módulos locales
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

# Clave W&B — Juanan usa WANDB_KEY en el .env
# os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SEED = 42

def main():
    if not wf.inicializar_apikey_wandb():
        return
    
    # Todas las variables: no eliminar correladas por ahora
    X, y = cargar_dataset_general(eliminar_correladas=False)

    # Split temporal 80/20
    X_train_full, X_val, y_train_full, y_val = split_temporal(X, y)
    
    # 2. Validación Cruzada
    cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
    f2_cv_scores = []
    f1_cv_scores = []
    recall_cv_scores = []

    clf_cv = xgb.XGBClassifier(
        n_estimators=10000,
        learning_rate=0.1,
        random_state=SEED,
        eval_metric="logloss",
    )

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        # Entrenar n - 1 grupos y predecimos el otro
        clf_cv.fit(X_fold_train, y_fold_train)
        
        y_fold_pred = clf_cv.predict(X_fold_val)

        # Guardamos los valores de f1-score y del recall 
        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))

    # Creamos el valor medio para saber la efectividad
    f2_score_mean = np.mean(f2_cv_scores)
    f1_score_mean = np.mean(f1_cv_scores)
    recall_score_mean = np.mean(recall_cv_scores)

    # 3. Entrenamos el modelo final con el 80% (train_full)
    clf = xgb.XGBClassifier(
        n_estimators=10000,
        learning_rate=0.1,
        random_state=SEED,
        eval_metric="logloss",
    )
    clf.fit(X_train_full, y_train_full)
    model_params = clf.get_params()

    # 4. Predecimos el 20% inicial (val)
    y_pred_val = clf.predict(X_val)
    y_prob_val = clf.predict_proba(X_val)[:, 1]
    metricas_val = evaluar_clasificacion(y_val, y_pred_val, y_prob_val, "Validación — XGBoost")

    # Inicializar Run
    run = wandb.init(
        entity=WANDB_ENTITY,
        name="XGBoost Experimento 2 (más iteraciones)",
        project=WANDB_PROJECT,
        config={
            **model_params,
            "split": "estratificado",
            "eliminar_correladas": False,
            "n_features": X.shape[1],
        },
    )

    # 5. Métricas (Loggeo explícito de floats para W&B)
    wandb.log({
        "val/f1": metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall": metricas_val["recall"],
        "val/accuracy": metricas_val["accuracy"],
        "val/roc_auc": metricas_val.get("roc_auc", 0),
        "val/f1_mean_cv": f1_score_mean,
        "val/f2_mean_cv": f2_score_mean,
        "val/recall_mean_cv": recall_score_mean,
    })

    # Visualizaciones detalladas
    wandb.sklearn.plot_classifier(
        clf,
        X_train_full,
        X_val,
        y_train_full,
        y_val,
        y_pred_val,
        clf.predict_proba(X_val),
        labels=["no_incendio", "incendio"],
        model_name="XGBoostClassifier",
        feature_names=X.columns.tolist(),
    )

    plot_roc(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
    plot_precision_recall(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
    plot_feature_importances(clf)

    run.finish()

if __name__ == "__main__":
    main()