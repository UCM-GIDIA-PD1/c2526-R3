import os
import sys
import numpy as np
import pandas as pd # Añadido para manejo de datos

import wandb
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
    # 1. Carga de datos
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False, logs=False)

    # 2. Partición inicial
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    #  VALIDACIÓN
    scaler_val = StandardScaler()
    X_train_scaled_init = scaler_val.fit_transform(X_train)
    X_val_scaled_init = scaler_val.transform(X_val)

    modelo_val = KNeighborsRegressor(n_neighbors=20)
    modelo_val.fit(X_train_scaled_init, y_train)
    y_pred_val = modelo_val.predict(X_val_scaled_init)

    # REENTRENAMIENTO (Train + Val) PARA TEST FINAL ---
    # Unimos los datos originales sin escalar
    X_trainval = pd.concat([X_train, X_val])
    y_trainval = pd.concat([y_train, y_val])

    # ESCALADO NUEVO
    scaler_final = StandardScaler()
    X_trainval_scaled = scaler_final.fit_transform(X_trainval)
    X_test_scaled = scaler_final.transform(X_test)

    modelo_final = KNeighborsRegressor(n_neighbors=20)
    modelo_final.fit(X_trainval_scaled, y_trainval)

    # Predicción final en TEST
    y_pred_test = modelo_final.predict(X_test_scaled)

    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "KNN_Refit_Corrected",
            "n_neighbors": 20,
            "features": list(X.columns),
            "split": "regresion_80_10_10",
        },
    )

    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — KNN", en_log=False)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — KNN (Post-Refit)", en_log=False)

    # Log de métricas
    wandb.log({
        **{f"val/{k}": v for k, v in metricas_val.items()},
        **{f"test/{k}": v for k, v in metricas_test.items()}
    })

    # Gráfico
    data = [[x, y] for (x, y) in zip(y_test, y_pred_test)]
    table = wandb.Table(data=data, columns=["FRP Real", "FRP Predicho"])
    wandb.log({"scatter_plot": wandb.plot.scatter(table, "FRP Real", "FRP Predicho", title="Test: Real vs Predicho")})

    run.finish()

if __name__ == "__main__":
    main()

#Conclusiones:
#Valores de R^2 negativos en validación, e incluso un valor de 0.03 en el conjunto test
#indican la posibilidad de que este algoritmo sea inutilizable para este tipo de conjunto de 
#datos, sin embargo haremos otra prueba más.

#Siguiente experimento:
# 1. Eliminar outliers:
# El algoritmo KNN predice basándose en el promedio de los vecinos más cercanos. Al tener 
# muy pocos ejemplos por encima de 150 (como se ve en el gráfico), el modelo no tiene suficientes 
# "referencias" para aprender esos valores extremos y siempre tenderá a predecir hacia la media 
# del grupo más denso (por eso se ven "aplanados" en el eje Y)
# 2. Hacer selección de variables:
#El algoritmo KNN no es robusto con un montón de variables. 
