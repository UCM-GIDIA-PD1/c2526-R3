import os
import sys

import wandb
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SequentialFeatureSelector

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion
import numpy as np

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "KNN"

def main():
    # 1. Cargar y limpiar datos
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False, eliminar_outliers=True)
    
    # Filtrar outliers extremos antes del split
    indices_outliers = y_log[y_log > 150].index
    X = X.drop(index=indices_outliers)
    y_log = y_log.drop(index=indices_outliers)

    # 2. Particiones
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    # 3. Escalado (Fundamental para SFS y KNN)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    # 4. Selección de características (SFS)
    knn_base = KNeighborsRegressor(n_neighbors=5)
    sfs = SequentialFeatureSelector(
        knn_base, 
        n_features_to_select="auto", 
        direction='forward', 
        scoring='neg_mean_squared_error',
        cv=5
    )
    
    sfs.fit(X_train_scaled, y_train)
    
    # Reducir los datasets a las características seleccionadas
    X_train_sfs = sfs.transform(X_train_scaled)
    X_val_sfs = sfs.transform(X_val_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)
    
    features_seleccionadas = X.columns[sfs.get_support()].tolist()

    # 5. Entrenar modelo final con las variables elegidas
    modelo_final = KNeighborsRegressor(n_neighbors=20)
    modelo_final.fit(X_train_sfs, y_train)

    y_pred_val = modelo_final.predict(X_val_sfs)
    y_pred_test = modelo_final.predict(X_test_sfs)

    # 6. W&B
    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "KNN_with_SFS",
            "n_neighbors": 5,
            "n_features_selected": 10,
            "selected_features": features_seleccionadas,
            "split": "regresion_80_10_10"
        },
    )

    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — KNN SFS", en_log=True)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — KNN SFS", en_log=True)

    # Log de métricas
    wandb.log({**{f"val/{k}": v for k, v in metricas_val.items()}, 
               **{f"test/{k}": v for k, v in metricas_test.items()}})
    
    data = [[x, y] for (x, y) in zip(y_test, y_pred_test)]
    table = wandb.Table(data=data, columns=["FRP Real", "FRP Predicho"])
    wandb.log({"scatter_plot": wandb.plot.scatter(table, "FRP Real", "FRP Predicho", title="Test: Real vs Predicho")})

    run.finish()

if __name__ == "__main__":
    main()

#Habiendo hecho selección de variables, eliminación de outliers y logaritmos, 
#el coeficiente de determinación incluso se reduce. 
#No creemos que poniendo un hiperparámetro para un mayor valor de vecinos cercanos, el 
#coeficiente vaya a mejorar tanto como para que el modelo sea fiable. 