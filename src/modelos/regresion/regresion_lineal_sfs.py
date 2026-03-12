import os
import wandb
import numpy as np
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.feature_selection import SequentialFeatureSelector

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios

# Clave W&B — Sofía usa WANDB_KEY en su .env
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")


def main():
    # Carga datos usando el pipeline compartido del proyecto
    X, y_log = cargar_dataset_incendios()

    # Sofía trabaja con frp_mean directamente (sin log), recuperamos la escala original
    y = np.expm1(y_log)

    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Escalado
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    # Forward Selection
    modelo_base = LinearRegression()
    sfs = SequentialFeatureSelector(
        modelo_base,
        n_features_to_select="auto",
        tol=0.001,
        direction="forward",
        scoring="r2",
        cv=5,
        n_jobs=-1
    )
    sfs.fit(X_train_scaled, y_train)

    X_train_sfs = sfs.transform(X_train_scaled)
    X_test_sfs  = sfs.transform(X_test_scaled)
    features_seleccionadas = X.columns[sfs.get_support()].tolist()

    # Entrenamiento
    modelo_final = LinearRegression()
    modelo_final.fit(X_train_sfs, y_train)
    y_pred = modelo_final.predict(X_test_sfs)

    # Ajuste statsmodels para tests de hipótesis
    X_train_sm = sm.add_constant(X_train_sfs)
    modelo_sm  = sm.OLS(y_train, X_train_sm).fit()
    residuos   = modelo_sm.resid

    # W&B
    run = wandb.init(
        entity="pd1-c2526-team3",
        project="regLinealMultiple",
        config={
            "model":                  "LinearRegression_SFS",
            "features_originales":    list(X.columns),
            "features_seleccionadas": features_seleccionadas,
            "n_features_final":       len(features_seleccionadas),
            "test_size":              0.2
        }
    )

    # Tests de hipótesis
    stat_shapiro, p_shapiro = (
        stats.shapiro(residuos) if len(residuos) < 5000 else (0, 0)
    )
    test_bp = sms.het_breuschpagan(residuos, X_train_sm)
    p_bp    = test_bp[1]

    wandb.run.summary["p_valor_shapiro"]       = p_shapiro
    wandb.run.summary["p_valor_breusch_pagan"] = p_bp

    # Métricas
    r2_test  = r2_score(y_test, y_pred)
    r2_train = modelo_sm.rsquared

    wandb.log({
        "r2_test":  r2_test,
        "r2_train": r2_train,
        "mse":      mean_squared_error(y_test, y_pred),
        "mae":      mean_absolute_error(y_test, y_pred)
    })

    # Importancia de variables seleccionadas
    coef_data = [[f, c] for f, c in zip(features_seleccionadas, modelo_final.coef_)]
    table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
    wandb.log({"feature_importance": wandb.plot.bar(
        table, "Feature", "Coefficient", title="Pesos del Modelo Seleccionado"
    )})

    run.finish()


if __name__ == "__main__":
    main()