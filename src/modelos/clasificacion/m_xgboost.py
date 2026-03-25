import os
import sys

import wandb
import xgboost as xgb
from wandb.sklearn import (
    plot_class_proportions,
    plot_learning_curve,
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general
from modelos.utils.particiones import split_estratificado
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf

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

    X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)

    clf = xgb.XGBClassifier(
        n_estimators=10000,
        learning_rate=0.1,
        random_state=SEED,
        eval_metric="logloss",
    )
    clf.fit(X_train, y_train)
    model_params = clf.get_params()

    y_pred_val = clf.predict(X_val)
    y_prob_val = clf.predict_proba(X_val)[:, 1]
    metricas_val = evaluar_clasificacion(y_val, y_pred_val, y_prob_val, "Validación — XGBoost")

    y_pred_test = clf.predict(X_test)
    y_prob_test = clf.predict_proba(X_test)[:, 1]
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test, "Test — XGBoost")

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

    wandb.log({
        "val/f1": metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall": metricas_val["recall"],
        "val/accuracy": metricas_val["accuracy"],
        "val/roc_auc": metricas_val.get("roc_auc", 0),
        "test/f1": metricas_test["f1"],
        "test/precision": metricas_test["precision"],
        "test/recall": metricas_test["recall"],
        "test/accuracy": metricas_test["accuracy"],
        "test/roc_auc": metricas_test.get("roc_auc", 0),
    })

    wandb.sklearn.plot_classifier(
        clf,
        X_train,
        X_val,
        y_train,
        y_val,
        y_pred_val,
        clf.predict_proba(X_val),
        labels=["no_incendio", "incendio"],
        model_name="XGBoostClassifier",
        feature_names=X.columns.tolist(),
    )

    plot_class_proportions(y_train, y_val, ["no_incendio", "incendio"])
    plot_learning_curve(clf, X_train, y_train)
    plot_roc(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
    plot_precision_recall(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
    plot_feature_importances(clf)

    run.finish()


if __name__ == "__main__":
    main()
