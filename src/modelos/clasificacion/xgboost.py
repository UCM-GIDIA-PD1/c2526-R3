import os
import wandb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from wandb.sklearn import (
    plot_class_proportions, plot_learning_curve,
    plot_roc, plot_precision_recall, plot_feature_importances
)

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general

# Clave W&B — usa WANDB_KEY en el .env
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")


def main():
    X, y = cargar_dataset_general(eliminar_correladas=False)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42
    )

    clf = xgb.XGBClassifier(n_estimators=10000, learning_rate=0.1)
    clf.fit(X_train, y_train)
    model_params = clf.get_params()

    y_pred   = clf.predict(X_test)
    y_probas = clf.predict_proba(X_test)

    run = wandb.init(
        entity="pd1-c2526-team3",
        name="XGBoost Experimento 2 (más iteraciones)",
        project="XGboost",
        config=model_params
    )

    wandb.sklearn.plot_classifier(
        clf, X_train, X_test, y_train, y_test, y_pred, y_probas,
        labels=X.columns,
        model_name="XGBoostClassifier",
        feature_names=X.columns
    )

    plot_class_proportions(y_train, y_test, X.columns)
    plot_learning_curve(clf, X_train, y_train)
    plot_roc(y_test, y_probas, X.columns)
    plot_precision_recall(y_test, y_probas, X.columns)
    plot_feature_importances(clf)

    run.finish()


if __name__ == "__main__":
    main()