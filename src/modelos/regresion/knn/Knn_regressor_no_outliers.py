import os
import sys
import numpy as np
import pandas as pd
import wandb
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SequentialFeatureSelector

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "KNN"

def main():
    # 1. Cargar y limpiar datos
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False, logs=False)
    
    indices_outliers = y_log[y_log > 150].index
    X = X.drop(index=indices_outliers)
    y_log = y_log.drop(index=indices_outliers)

    # 2. Particiones
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    # 3. Escalado inicial (para Validación y SFS)
    scaler_init = StandardScaler()
    X_train_scaled = scaler_init.fit_transform(X_train)
    X_val_scaled = scaler_init.transform(X_val)

    # 4. Selección de características (SFS)
    knn_base = KNeighborsRegressor(n_neighbors=5)
    sfs = SequentialFeatureSelector(
        knn_base, 
        n_features_to_select="auto", 
        direction='forward', 
        scoring='neg_mean_squared_error',
        cv=5
    )
    
    print("Seleccionando características...")
    sfs.fit(X_train_scaled, y_train)
    features_seleccionadas = X.columns[sfs.get_support()].tolist()

    # --- PASO A: EVALUACIÓN DE VALIDACIÓN (Modelo con solo Train) ---
    X_train_sfs = sfs.transform(X_train_scaled)
    X_val_sfs = sfs.transform(X_val_scaled)
    
    modelo_val = KNeighborsRegressor(n_neighbors=20)
    modelo_val.fit(X_train_sfs, y_train)
    y_pred_val = modelo_val.predict(X_val_sfs)
    
    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — Pre-Refit", en_log=False)

    # --- PASO B: REENTRENAMIENTO (Train + Val) PARA TEST FINAL ---
    X_train_val = pd.concat([X_train, X_val])
    y_train_val = pd.concat([y_train, y_val])
    
    # Nuevo escalador con el bloque completo
    scaler_final = StandardScaler()
    X_train_val_scaled = scaler_final.fit_transform(X_train_val)
    X_test_scaled = scaler_final.transform(X_test)
    
    # Aplicar la selección de variables previa
    X_train_val_sfs = sfs.transform(X_train_val_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)
    
    modelo_final = KNeighborsRegressor(n_neighbors=20)
    modelo_final.fit(X_train_val_sfs, y_train_val)
    y_pred_test = modelo_final.predict(X_test_sfs)
    
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test Final — Post-Refit", en_log=False)

    # 5. W&B Logging
    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "KNN_SFS_Refit",
            "n_neighbors": 20,
            "features": features_seleccionadas,
            "n_features": len(features_seleccionadas)
        },
    )

    # Log de ambos diccionarios de métricas
    wandb.log({
        **{f"val/{k}": v for k, v in metricas_val.items()},
        **{f"test/{k}": v for k, v in metricas_test.items()}
    })
    
    # Gráfico de dispersión para el Test Final
    data = [[x, y] for (x, y) in zip(y_test, y_pred_test)]
    table = wandb.Table(data=data, columns=["FRP Real", "FRP Predicho"])
    wandb.log({"scatter_test": wandb.plot.scatter(table, "FRP Real", "FRP Predicho", title="Test: Real vs Predicho (Modelo Refit)")})

    run.finish()

if __name__ == "__main__":
    main()
    import os
import sys
import numpy as np
import pandas as pd
import wandb
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SequentialFeatureSelector

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "KNN"

def main():
    # 1. Cargar y limpiar datos
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False, logs=False)
    
    indices_outliers = y_log[y_log > 150].index
    X = X.drop(index=indices_outliers)
    y_log = y_log.drop(index=indices_outliers)

    # 2. Particiones
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    # 3. Escalado inicial (para Validación y SFS)
    scaler_init = StandardScaler()
    X_train_scaled = scaler_init.fit_transform(X_train)
    X_val_scaled = scaler_init.transform(X_val)

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
    features_seleccionadas = X.columns[sfs.get_support()].tolist()

    # SOLO TRAIN
    X_train_sfs = sfs.transform(X_train_scaled)
    X_val_sfs = sfs.transform(X_val_scaled)
    
    modelo_val = KNeighborsRegressor(n_neighbors=20)
    modelo_val.fit(X_train_sfs, y_train)
    y_pred_val = modelo_val.predict(X_val_sfs)
    
    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — Pre-Refit", en_log=False)

    # REENTRENAMIENTO (Train + Val)
    X_train_val = pd.concat([X_train, X_val])
    y_train_val = pd.concat([y_train, y_val])
    
    # Nuevo escalar
    scaler_final = StandardScaler()
    X_train_val_scaled = scaler_final.fit_transform(X_train_val)
    X_test_scaled = scaler_final.transform(X_test)
    
    # Aplicar la selección de variables previa
    X_train_val_sfs = sfs.transform(X_train_val_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)
    
    modelo_final = KNeighborsRegressor(n_neighbors=20)
    modelo_final.fit(X_train_val_sfs, y_train_val)
    y_pred_test = modelo_final.predict(X_test_sfs)
    
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test Final — Post-Refit", en_log=False)

    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "KNN_SFS_Refit",
            "n_neighbors": 20,
            "features": features_seleccionadas,
            "n_features": len(features_seleccionadas)
        },
    )

    # Log de ambos diccionarios de métricas
    wandb.log({
        **{f"val/{k}": v for k, v in metricas_val.items()},
        **{f"test/{k}": v for k, v in metricas_test.items()}
    })
    
    # Gráfico de dispersión para el Test Final
    data = [[x, y] for (x, y) in zip(y_test, y_pred_test)]
    table = wandb.Table(data=data, columns=["FRP Real", "FRP Predicho"])
    wandb.log({"scatter_test": wandb.plot.scatter(table, "FRP Real", "FRP Predicho", title="Test: Real vs Predicho (Modelo Refit)")})

    run.finish()

if __name__ == "__main__":
    main()

#Habiendo hecho selección de variables, eliminación de outliers y logaritmos, 
#el coeficiente de determinación ha aumentado ligeramente, 0.03 en test y 0.15 en validacion (siendo ahora 0.12)
#Sin embargo, no creemos que poniendo un hiperparámetro para un mayor valor de vecinos cercanos, el 
#coeficiente vaya a mejorar tanto como para que el modelo sea fiable. 