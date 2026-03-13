import os
import sys

import wandb
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "KNN"

def main():
    # Todas las variables
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False)

    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    modelo = KNeighborsRegressor(n_neighbors = 20)
    modelo.fit(X_train_scaled, y_train)

    y_pred_val = modelo.predict(X_val_scaled)
    y_pred_test = modelo.predict(X_test_scaled)

    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "LinearRegression",
            "features": list(X.columns),
            "split": "regresion_80_10_10",
            "eliminar_correladas": False,
        },
    )

    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — KNN", en_log=True)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — KNN", en_log=True)

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

    data = [[x, y] for (x, y) in zip(y_test, y_pred_test)]
    table = wandb.Table(data=data, columns=["FRP Real", "FRP Predicho"])
    wandb.log({"scatter_plot": wandb.plot.scatter(table, "FRP Real", "FRP Predicho", title="Test: Real vs Predicho")})

    run.finish()


if __name__ == "__main__":
    main()

#Siguiente experimento:
# 1. Eliminar outliers:
# El algoritmo KNN predice basándose en el promedio de los vecinos más cercanos. Al tener 
# muy pocos ejemplos por encima de 150 (como se ve en el gráfico), el modelo no tiene suficientes 
# "referencias" para aprender esos valores extremos y siempre tenderá a predecir hacia la media 
# del grupo más denso (por eso se ven "aplanados" en el eje Y)
# 2. Hacer selección de variables:
#El algoritmo KNN no es robusto con un montón de variables. 
