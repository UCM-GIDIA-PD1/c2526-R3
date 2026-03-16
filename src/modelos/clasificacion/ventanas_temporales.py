import os
import sys
import wandb
import numpy as np
import pandas as pd
import matplotlib
from datetime import timedelta
from xgboost import XGBClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, f1_score 
import matplotlib.pyplot as plt
from wandb.sklearn import (
    plot_class_proportions,
    plot_learning_curve,
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

# Para que haga todo en memoria y no se sincronicen (si lo hace colapsa)
matplotlib.use('Agg')

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general_con_tiempos
from modelos.utils.metricas import evaluar_clasificacion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")
os.environ["WANDB_MODE"] = "online"

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "ModelosTemporales"
SEED = 42

def encontrar_mejor_umbral(y_true, y_prob):
    umbrales = np.arange(0.01, 1.0, 0.01)
    mejor_f1, mejor_umbral = 0, 0.5
    for umbral in umbrales:
        pred = (y_prob >= umbral).astype(int)
        f1 = f1_score(y_true, pred, zero_division=0)
        if f1 > mejor_f1:
            mejor_f1, mejor_umbral = f1, umbral
    return mejor_umbral

def crear_features_temporales(df, radio_km=5, ventana_dias=7):
    df = df.sort_values('date').reset_index(drop=True)
    df['incendios_recientes'] = 0
    df['dias_ultimo_incendio'] = np.nan
    
    R = 6371
    df['lat_rad'] = np.radians(df['lat'])
    df['lon_rad'] = np.radians(df['lon'])
    
    for i, fila in df.iterrows():
        fecha_actual = fila['date']
        fecha_inicio_ventana = fecha_actual - timedelta(days=ventana_dias)
        
        mascota_temporal = (df['date'] >= fecha_inicio_ventana) & (df['date'] < fecha_actual) & (df['incendio'] == 1)
        incendios_pasados = df[mascota_temporal]
        
        if not incendios_pasados.empty:
            lat1, lon1 = fila['lat_rad'], fila['lon_rad']
            lat2s = incendios_pasados['lat_rad'].values
            lon2s = incendios_pasados['lon_rad'].values
            
            dlat = lat2s - lat1
            dlon = lon2s - lon1
            a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2s) * np.sin(dlon/2)**2
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
            distancias = R * c
            
            indices_cercanos = np.where(distancias <= radio_km)[0]
            if len(indices_cercanos) > 0:
                incendios_cercanos = incendios_pasados.iloc[indices_cercanos]
                df.at[i, 'incendios_recientes'] = len(incendios_cercanos)
                df.at[i, 'dias_ultimo_incendio'] = (fecha_actual - incendios_cercanos['date'].max()).days
                
    df = df.drop(['lat_rad', 'lon_rad'], axis=1)
    return df

def split_temporal(X, y, test_size=0.2, val_size=0.1):
    n_total = len(X)
    n_test = int(n_total * test_size)
    n_val = int(n_total * val_size)
    n_train = n_total - n_test - n_val

    X_train, y_train = X.iloc[:n_train], y.iloc[:n_train]
    X_val, y_val = X.iloc[n_train:n_train+n_val], y.iloc[n_train:n_train+n_val]
    X_test, y_test = X.iloc[n_train+n_val:], y.iloc[n_train+n_val:]
    
    return X_train, X_val, X_test, y_train, y_val, y_test

def seleccionar_modelo(y_train):
    print("\n--- Menú de Selección de Modelo ---")
    print("1. XGBoost (Estándar)")
    print("2. XGBoost (Con scale_pos_weight)")
    print("3. Random Forest (Balanceado)")
    print("4. Búsqueda Grid Mixta (60 modelos)")
    print("5. Búsqueda Grid Diferente Enfoque Máximo f1-score (54 modelos)")
    print("6. Búsqueda Grid Mixta con Optimización de Umbral (60 modelos)")
    
    opcion = input("Elige el modelo a entrenar (1/2/3/4/5/6): ")
    
    if opcion == '2':
        negativos = (y_train == 0).sum()
        positivos = (y_train == 1).sum()
        ratio = negativos / positivos
        
        clf = XGBClassifier(
            n_estimators=1000,
            learning_rate=0.05,
            scale_pos_weight=ratio,
            random_state=SEED,
            eval_metric="logloss",
        )
        return [(clf, "XGBoost_Balanceado")]
        
    elif opcion == '3':
        clf = RandomForestClassifier(
            n_estimators=500,
            class_weight="balanced",
            random_state=SEED,
            n_jobs=-1
        )
        return [(clf, "RandomForest_Balanceado")]
        
    elif opcion == '4':
        modelos = []
        negativos = (y_train == 0).sum()
        positivos = (y_train == 1).sum()
        ratio = negativos / positivos

        for depth in [3, 5, 7, 9]:
            for lr in [0.01, 0.05, 0.1]:
                for n_est in [100, 300]:
                    clf_est = XGBClassifier(
                        n_estimators=n_est,
                        learning_rate=lr,
                        max_depth=depth,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_est, f"XGBoost_Est_d{depth}_lr{lr}_n{n_est}"))
                    clf_bal = XGBClassifier(
                        n_estimators=n_est,
                        learning_rate=lr,
                        max_depth=depth,
                        scale_pos_weight=ratio,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_bal, f"XGBoost_Bal_d{depth}_lr{lr}_n{n_est}"))

        for depth in [None, 5, 10, 15]:
            for n_est in [100, 300, 500]:
                clf_rf = RandomForestClassifier(
                    n_estimators=n_est,
                    max_depth=depth,
                    class_weight="balanced",
                    random_state=SEED,
                    n_jobs=-1
                )
                depth_str = depth if depth is not None else "None"
                modelos.append((clf_rf, f"RandomForest_Bal_d{depth_str}_n{n_est}"))
                
        return modelos
        
    elif opcion == '5':
        modelos = []
        negativos = (y_train == 0).sum()
        positivos = (y_train == 1).sum()
        ratio = negativos / positivos
        
        for depth in [4, 6]:
            for lr in [0.05, 0.1]:
                for gamma in [0, 1, 5]: 
                    clf_est = XGBClassifier(
                        n_estimators=200,
                        learning_rate=lr,
                        max_depth=depth,
                        gamma=gamma,
                        subsample=0.8,
                        colsample_bytree=0.8,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_est, f"XGB_Est_d{depth}_lr{lr}_g{gamma}_reg"))

        for depth in [4, 6]:
            for lr in [0.05, 0.1]:
                for max_delta in [1, 5, 10]: 
                    clf_bal = XGBClassifier(
                        n_estimators=200,
                        learning_rate=lr,
                        max_depth=depth,
                        scale_pos_weight=ratio,
                        max_delta_step=max_delta,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_bal, f"XGB_Bal_d{depth}_lr{lr}_md{max_delta}"))

        for depth in [8, 12, 20]:
            for min_leaf in [1, 5, 10]:
                for n_est in [200, 400]:
                    clf_rf = RandomForestClassifier(
                        n_estimators=n_est, 
                        max_depth=depth, 
                        min_samples_leaf=min_leaf, 
                        class_weight="balanced_subsample", 
                        random_state=SEED, 
                        n_jobs=-1
                    )
                    modelos.append((clf_rf, f"RF_BalSub_d{depth}_ml{min_leaf}_n{n_est}"))
                    
        return modelos

    elif opcion == '6':
        modelos = []
        negativos = (y_train == 0).sum()
        positivos = (y_train == 1).sum()
        ratio = negativos / positivos

        for depth in [3, 5, 7, 9]:
            for lr in [0.01, 0.05, 0.1]:
                for n_est in [100, 300]:
                    clf_est = XGBClassifier(
                        n_estimators=n_est,
                        learning_rate=lr,
                        max_depth=depth,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_est, f"XGB_Est_d{depth}_lr{lr}_n{n_est}_Umbral_Optimo"))
                    
                    clf_bal = XGBClassifier(
                        n_estimators=n_est,
                        learning_rate=lr,
                        max_depth=depth,
                        scale_pos_weight=ratio,
                        random_state=SEED,
                        eval_metric="logloss"
                    )
                    modelos.append((clf_bal, f"XGB_Bal_d{depth}_lr{lr}_n{n_est}_Umbral_Optimo"))

        for depth in [None, 5, 10, 15]:
            for n_est in [100, 300, 500]:
                clf_rf = RandomForestClassifier(
                    n_estimators=n_est,
                    max_depth=depth,
                    class_weight="balanced",
                    random_state=SEED,
                    n_jobs=-1
                )
                depth_str = depth if depth is not None else "None"
                modelos.append((clf_rf, f"RF_Bal_d{depth_str}_n{n_est}_Umbral_Optimo"))
                
        return modelos

    else:
        clf = XGBClassifier(
            n_estimators=1000,
            learning_rate=0.1,
            random_state=SEED,
            eval_metric="logloss"
        )
        return [(clf, "XGBoost_Estandar")]

def main():
    print("\n--- Configuración de Tiempo ---")
    entrada_dias = input("¿Cuántos días quieres? (Por defecto: 7): ")
    try:
        ventana_elegida = int(entrada_dias)
    except ValueError:
        print("Usando el valor por defecto de 7 días.")
        ventana_elegida = 7

    X, y = cargar_dataset_general_con_tiempos(eliminar_correladas=False)
    
    df_completo = pd.concat([X, y.rename('incendio')], axis=1)
    print(df_completo.columns.to_list())
    df_completo['date'] = pd.to_datetime(df_completo['date'])
    
    print(f"Calculando ventanas espacio-temporales ({ventana_elegida} días).")
    df_features = crear_features_temporales(df_completo, radio_km=10, ventana_dias=ventana_elegida)
    
    y_features = df_features['incendio']
    X_features = df_features.drop(['incendio', 'date', 'lat', 'lon'], axis=1)

    X_train, X_val, X_test, y_train, y_val, y_test = split_temporal(X_features, y_features)

    modelos_a_entrenar = seleccionar_modelo(y_train)
    
    for clf, model_name in modelos_a_entrenar:
        print(f"\nEntrenando {model_name}...")
        
        clf.fit(X_train, y_train)
        model_params = clf.get_params()

        y_prob_val = clf.predict_proba(X_val)[:, 1]
        
        if "Umbral_Optimo" in model_name:
            mejor_umbral = encontrar_mejor_umbral(y_val, y_prob_val)
            print(f"El umbral óptimo es {mejor_umbral}")
        else:
            mejor_umbral = 0.5 
            
        y_pred_val = (y_prob_val >= mejor_umbral).astype(int)
        metricas_val = evaluar_clasificacion(y_val, y_pred_val, y_prob_val, f"Validación — {model_name}")

        y_prob_test = clf.predict_proba(X_test)[:, 1]
        y_pred_test = (y_prob_test >= mejor_umbral).astype(int)
        metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test, f"Test — {model_name}")

        # Añadimos la ventana_elegida al nombre para verlo claro en W&B
        run = wandb.init(
            entity=WANDB_ENTITY,
            name=f"{model_name} - {ventana_elegida}d Temp",
            project=WANDB_PROJECT,
            config={
                **model_params,
                "split": "temporal",
                "eliminar_correladas": False,
                "n_features": X_features.shape[1],
                "arquitectura": model_name,
                "radio_km": 10,
                "ventana_dias": ventana_elegida,
                "umbral_utilizado": mejor_umbral 
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

        cm = confusion_matrix(y_test, y_pred_test, normalize='true')  
        fig, ax = plt.subplots(figsize=(6,5))
        im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
        ax.figure.colorbar(im, ax=ax)
        ax.set(xticks=np.arange(cm.shape[1]), yticks=np.arange(cm.shape[0]),
               xticklabels=["no incendio", "incendio"], yticklabels=["no incendio", "incendio"],
               ylabel='Actual', xlabel='Predicho')
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, f"{cm[i, j]:.2%}", ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        ax.set_title(f'Matriz de Confusión Normalizada (Test)\n{model_name} - {ventana_elegida}d - Umbral: {mejor_umbral}')        
        wandb.log({"test/confusion_matrix_normalized": wandb.Image(fig)})
        plt.close(fig)

        if "XGBoost" in model_name or "XGB" in model_name:
            wandb.sklearn.plot_classifier(
                clf, X_train, X_val, y_train, y_val,
                y_pred_val, clf.predict_proba(X_val),
                labels=["no_incendio", "incendio"],
                model_name=model_name,
                feature_names=X_features.columns.tolist(),
            )
            plot_class_proportions(y_train, y_val, ["no_incendio", "incendio"])
            plot_learning_curve(clf, X_train, y_train)
            plot_roc(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
            plot_precision_recall(y_val, clf.predict_proba(X_val), ["no_incendio", "incendio"])
            plot_feature_importances(clf)

        run.finish()

if __name__ == "__main__":
    main()