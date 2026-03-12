import os
import wandb
import numpy as np
import yaml
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import fbeta_score
from sklearn.model_selection import train_test_split

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")


def wandb_init():
    wandb.init(
        entity="pd1-c2526-team3",
        project="arboles-decision-sweeps",
    )


def arboles_decision(X_train, X_test, y_train, y_test):
    wandb_init()
    config = wandb.config

    model = RandomForestClassifier(
        max_depth=config.max_depth,
        criterion=config.criterion,
        n_estimators=config.n_estimators,
        class_weight=config.class_weight,
        min_samples_leaf=config.min_samples_leaf,
        min_samples_split=config.min_samples_split,
        random_state=42
    )

    model.fit(X_train, y_train)

    y_pred   = model.predict(X_test)
    y_probas = model.predict_proba(X_test)

    nombres_clases = [str(c) for c in model.classes_]

    f1_5 = fbeta_score(y_test, y_pred, beta=1.5)
    wandb.log({"f1_5_score": f1_5})

    wandb.sklearn.plot_classifier(
        model, X_train, X_test, y_train, y_test, y_pred, y_probas,
        labels=nombres_clases,
        model_name="RandomForest",
        feature_names=X_train.columns.tolist()
    )

    wandb.finish()


def main():
    # Carga datos usando el pipeline compartido del proyecto
    # eliminar_correladas=False para mantener el comportamiento original de Esteban
    X, y = cargar_dataset_general(eliminar_correladas=False)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    def entrenamiento():
        arboles_decision(X_train, X_test, y_train, y_test)

    # Sweep — count=25 ejecuciones con búsqueda bayesiana de hiperparámetros
    wandb.agent(
        sweep_id="l241peqb",
        function=entrenamiento,
        count=25,
        entity="pd1-c2526-team3",
        project="arboles-decision-sweeps"
    )


if __name__ == "__main__":
    main()