import os
import sys

import wandb
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

# Clave W&B — Sofía usa WANDB_KEY en su .env
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "regLinealMultiple"


def main():
    # Todas las variables: no eliminar correladas por ahora
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False)

    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    modelo_base = LinearRegression()
    sfs = SequentialFeatureSelector(
        modelo_base,
        n_features_to_select="auto",
        tol=0.001,
        direction="forward",
        scoring="r2",
        cv=5,
        n_jobs=-1,
    )
    sfs.fit(X_train_scaled, y_train)

    mascara = sfs.get_support()
    features_seleccionadas = X.columns[mascara].tolist()
    X_train_sfs = sfs.transform(X_train_scaled)
    X_val_sfs = sfs.transform(X_val_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)

    modelo_final = LinearRegression()
    modelo_final.fit(X_train_sfs, y_train)

    y_pred_val = modelo_final.predict(X_val_sfs)
    y_pred_test = modelo_final.predict(X_test_sfs)

    X_train_sm = sm.add_constant(X_train_sfs)
    modelo_sm = sm.OLS(y_train, X_train_sm).fit()
    residuos = modelo_sm.resid

    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "LinearRegression_SFS",
            "features_originales": list(X.columns),
            "features_seleccionadas": features_seleccionadas,
            "n_features_final": len(features_seleccionadas),
            "split": "regresion_80_10_10",
            "eliminar_correladas": False,
        },
    )

    _, p_shapiro = stats.shapiro(residuos) if len(residuos) < 5000 else (0, 0)
    test_bp = sms.het_breuschpagan(residuos, X_train_sm)
    p_bp = test_bp[1]

    wandb.run.summary["p_valor_shapiro"] = p_shapiro
    wandb.run.summary["p_valor_breusch_pagan"] = p_bp
    wandb.run.summary["r2_train"] = modelo_sm.rsquared

    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — Regresión lineal SFS", en_log=True)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — Regresión lineal SFS", en_log=True)

    wandb.log({
        "val/rmse": metricas_val["rmse"],
        "val/mae": metricas_val["mae"],
        "val/r2": metricas_val["r2"],
        "val/rmse_mw": metricas_val.get("rmse_mw"),
        "val/mae_mw": metricas_val.get("mae_mw"),
        "test/rmse": metricas_test["rmse"],
        "test/mae": metricas_test["mae"],
        "test/r2": metricas_test["r2"],
        "test/rmse_mw": metricas_test.get("rmse_mw"),
        "test/mae_mw": metricas_test.get("mae_mw"),
    })

    coef_data = [[f, c] for f, c in zip(features_seleccionadas, modelo_final.coef_)]
    table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
    wandb.log({
        "feature_importance": wandb.plot.bar(
            table,
            "Feature",
            "Coefficient",
            title="Pesos del Modelo Seleccionado",
        )
    })

    run.finish()


if __name__ == "__main__":
    main()
