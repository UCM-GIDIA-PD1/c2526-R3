import os
import sys
import numpy as np
import pandas as pd # Importante para concatenar correctamente

import wandb
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

# Clave W&B
os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "regLinealMultiple"

def main():
    # 1. Cargar dataset
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False)

    # 2. Particiones
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    # VALIDACIÓN
    scaler_init = StandardScaler()
    X_train_scaled = scaler_init.fit_transform(X_train)
    X_val_scaled = scaler_init.transform(X_val)

    modelo_val = LinearRegression()
    modelo_val.fit(X_train_scaled, y_train)
    y_pred_val = modelo_val.predict(X_val_scaled)

    # REENTRENAMIENTO (Refit con Train + Val)
    X_trainval = pd.concat([X_train, X_val])
    y_trainval = pd.concat([y_train, y_val])

    # ESCALADO NUEVO
    scaler_final = StandardScaler()
    X_trainval_scaled = scaler_final.fit_transform(X_trainval)
    X_test_scaled = scaler_final.transform(X_test)

    modelo_final = LinearRegression()
    modelo_final.fit(X_trainval_scaled, y_trainval)

    # Predicción final en TEST
    y_pred_test = modelo_final.predict(X_test_scaled)

    X_trainval_sm = sm.add_constant(X_trainval_scaled)
    modelo_sm = sm.OLS(y_trainval.values, X_trainval_sm).fit()
    residuos = modelo_sm.resid

    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "LinearRegression_Refit_Corrected",
            "features": list(X.columns),
            "split": "regresion_80_10_10",
            "eliminar_correladas": False,
        },
    )

    # Tests estadísticos
    _, p_shapiro = stats.shapiro(residuos)
    test_bp = sms.het_breuschpagan(residuos, X_trainval_sm)
    p_bp = test_bp[1]

    wandb.run.summary["p_valor_shapiro"] = p_shapiro
    wandb.run.summary["p_valor_breusch_pagan"] = p_bp
    wandb.run.summary["r2_trainval"] = modelo_sm.rsquared

    # Métricas
    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — Regresión lineal", en_log=True)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — Regresión lineal (Post-Refit)", en_log=True)

    wandb.log({
        **{f"val/{k}": v for k, v in metricas_val.items()},
        **{f"test/{k}": v for k, v in metricas_test.items()}
    })

    # Importancia de variables (Coeficientes del modelo final)
    coef_data = [[f, c] for f, c in zip(X.columns, modelo_final.coef_)]
    table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
    wandb.log({"feature_importance": wandb.plot.bar(table, "Feature", "Coefficient", title="Pesos del Modelo Final")})

    run.finish()

if __name__ == "__main__":
    main()