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
from sklearn.neighbors import BallTree
import matplotlib.pyplot as plt
from wandb.sklearn import (
    plot_class_proportions,
    plot_learning_curve,
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

matplotlib.use('Agg')

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_general_con_tiempos
from modelos.utils.metricas import evaluar_clasificacion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")
os.environ["WANDB_MODE"] = "online"

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "ModelosTemporales"
SEED = 42
R = 6371

def encontrar_mejor_umbral(y_true, y_prob):
    if len(y_true) == 0:
        return 0.5
    umbrales = np.arange(0.01, 1.0, 0.01)
    mejor_f1, mejor_umbral = 0, 0.5
    for umbral in umbrales:
        pred = (y_prob >= umbral).astype(int)
        f1 = f1_score(y_true, pred, zero_division=0)
        if f1 > mejor_f1:
            mejor_f1, mejor_umbral = f1, umbral
    return mejor_umbral

import numpy as np
import h3

def crear_features_temp_multiple(df, ventanas_dias, param_espacial=None):
    df = df.sort_values('date').reset_index(drop=True)
    n = len(df)
    es_h3 = 'id_hexagono' in df.columns
    
    incendios_mask = df['incendio'] == 1
    incendios_idx = df[incendios_mask].index
    fechas_ord = df['date'].values.astype('datetime64[s]').astype(int) // 10**9
    incendios_fechas_ord = fechas_ord[incendios_idx]
    
    tree_inc = None
    hex_neighbors = None
    incendios_hex = None
    
    if not es_h3:
        coords = np.radians(df[['lat', 'lon']].values)
        incendios_coords = coords[incendios_idx]
        
    if param_espacial is not None and len(incendios_idx) > 0:
        if es_h3:
            unique_hexs = df['id_hexagono'].unique()
            hex_neighbors = {h: h3.grid_disk(h, param_espacial) for h in unique_hexs}
            incendios_hex = df.loc[incendios_idx, 'id_hexagono'].values
        else:
            radio_rad = np.radians(param_espacial / R)
            tree_inc = BallTree(incendios_coords, metric='haversine')
            
    for w in ventanas_dias:
        col_inc = f'incendios_recientes_{w}d'
        col_dias = f'dias_ultimo_incendio_{w}d'
        df[col_inc] = 0
        df[col_dias] = np.nan
        
    for i in range(n):
        fecha_actual_ord = fechas_ord[i]
        
        if param_espacial is not None:
            if es_h3:
                hex_actual = df.at[i, 'id_hexagono']
                vecinos = hex_neighbors[hex_actual]
                idx_rad = np.where(np.isin(incendios_hex, list(vecinos)))[0]
                
                if len(idx_rad) == 0:
                    continue
                incendios_cercanos_idx = incendios_idx[idx_rad]
                fechas_cercanas_ord = incendios_fechas_ord[idx_rad]
                
            else:
                idx_rad = tree_inc.query_radius([coords[i]], r=radio_rad)[0]
                if len(idx_rad) == 0:
                    continue
                incendios_cercanos_idx = incendios_idx[idx_rad]
                fechas_cercanas_ord = incendios_fechas_ord[idx_rad]
        else:
            incendios_cercanos_idx = incendios_idx
            fechas_cercanas_ord = incendios_fechas_ord
            
        for w in ventanas_dias:
            fecha_limite = fecha_actual_ord - w * 86400
            mask = (fechas_cercanas_ord >= fecha_limite) & (fechas_cercanas_ord < fecha_actual_ord)
            if mask.any():
                df.at[i, f'incendios_recientes_{w}d'] = mask.sum()
                ultima_fecha = np.max(fechas_cercanas_ord[mask])
                df.at[i, f'dias_ultimo_incendio_{w}d'] = (fecha_actual_ord - ultima_fecha) / 86400
    return df


def crear_features_temp(df, param_espacial=None):
    df = df.sort_values('date').reset_index(drop=True)
    n = len(df)
    es_h3 = 'id_hexagono' in df.columns
    
    incendios_mask = df['incendio'] == 1
    incendios_idx = df[incendios_mask].index
    incendios_fechas = df.loc[incendios_idx, 'date'].values
    incendios_anios = incendios_fechas.astype('datetime64[Y]').astype(int) + 1970
    incendios_meses = incendios_fechas.astype('datetime64[M]').astype(int) % 12 + 1
    incendios_fechas_ord = incendios_fechas.astype('datetime64[s]').astype(int) // 10**9
    fechas_ord = df['date'].values.astype('datetime64[s]').astype(int) // 10**9
    
    tree_inc = None
    hex_neighbors = None
    
    if not es_h3:
        coords = np.radians(df[['lat', 'lon']].values)
        incendios_coords = coords[incendios_idx]
        
    if param_espacial is not None and len(incendios_idx) > 0:
        if es_h3:
            unique_hexs = df['id_hexagono'].unique()
            hex_neighbors = {h: h3.grid_disk(h, param_espacial) for h in unique_hexs}
        else:
            radio_rad = np.radians(param_espacial / R)
            tree_inc = BallTree(incendios_coords, metric='haversine')
    
    df['incendios_estacionales'] = 0
    df['dias_ultimo_incendio_estacional'] = np.nan
    
    for i in range(n):
        fecha_actual = df.loc[i, 'date']
        mes_actual = fecha_actual.month
        año_actual = fecha_actual.year
        fecha_actual_ord = fechas_ord[i]
        
        mask_mes = (incendios_meses == mes_actual) & (incendios_anios < año_actual)
        if not mask_mes.any():
            continue
            
        idx_candidatos = incendios_idx[mask_mes]
        fechas_candidatos_ord = incendios_fechas_ord[mask_mes]
        
        if param_espacial is not None:
            if es_h3:
                hex_actual = df.at[i, 'id_hexagono']
                vecinos = hex_neighbors[hex_actual]
                candidatos_hex = df.loc[idx_candidatos, 'id_hexagono'].values
                
                idx_rad = np.where(np.isin(candidatos_hex, list(vecinos)))[0]
                if len(idx_rad) == 0:
                    continue
                idx_filtrados = idx_candidatos[idx_rad]
                fechas_filtradas = fechas_candidatos_ord[idx_rad]
            else:
                coords_candidatos = incendios_coords[mask_mes]
                if len(coords_candidatos) > 0:
                    tree_cand = BallTree(coords_candidatos, metric='haversine')
                    idx_rad = tree_cand.query_radius([coords[i]], r=radio_rad)[0]
                    if len(idx_rad) == 0:
                        continue
                    idx_filtrados = idx_candidatos[idx_rad]
                    fechas_filtradas = fechas_candidatos_ord[idx_rad]
                else:
                    continue
        else:
            idx_filtrados = idx_candidatos
            fechas_filtradas = fechas_candidatos_ord
            
        if len(idx_filtrados) > 0:
            df.at[i, 'incendios_estacionales'] = len(idx_filtrados)
            ultima_fecha = np.max(fechas_filtradas)
            df.at[i, 'dias_ultimo_incendio_estacional'] = (fecha_actual_ord - ultima_fecha) / 86400
            
    return df

def split_temporal(X, y, test_size=0.2, val_size=0.1):
    n_total = len(X)
    n_val = int(n_total * val_size)
    n_test = int(n_total * test_size)
    
    if n_val == 0:
        n_val = 1
    if n_test == 0:
        n_test = 1
    n_train = n_total - n_test - n_val
    if n_train <= 0:
        n_train = 1
        n_val = max(1, (n_total - n_train) // 2)
        n_test = n_total - n_train - n_val

    X_train, y_train = X.iloc[:n_train], y.iloc[:n_train]
    X_val, y_val = X.iloc[n_train:n_train+n_val], y.iloc[n_train:n_train+n_val]
    X_test, y_test = X.iloc[n_train+n_val:], y.iloc[n_train+n_val:]
    
    return X_train, X_val, X_test, y_train, y_val, y_test

def seleccionar_modelo(y_train):
    print("\nMenú de Selección de Modelo")
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

def menu_ventanas_temporales(df):
    es_h3 = 'id_hexagono' in df.columns

    if es_h3:
        print("...Trabajando con dataset de hexágonos (H3)...")
    print("\nConfiguración de ventanas espacio-temporales")
    print("1. Temporal (días hacia atrás)")
    print("2. Estacional (mismo mes en todos los años anteriores)")
    print("3. Estacional - Temporal (combina ambas)")
    
    opcion_tipo = input("Opción (1/2/3): ").strip()
    
    if opcion_tipo == '1':
        incluir_espacial = input("¿Incluir componente espacial? (s/n): ").strip().lower()
        espacial = incluir_espacial == 's'
        radio_km = None
        if espacial:
            if es_h3:
                radio_input = input("Niveles de vecindad H3 (k-rings, 0 = solo el propio hexágono, por defecto 0): ").strip()
                param_espacial = int(radio_input) if radio_input else 0
            else:
                radio_input = input("Radio en km (por defecto 10): ").strip()
                param_espacial = float(radio_input) if radio_input else 10.0
        
        opcion_ventanas = input("¿Una o más ventanas? (1/más): ").strip()
        if opcion_ventanas == '1':
            valor = input("Valor de la ventana (días): ").strip()
            try:
                ventanas = [int(valor)]
            except:
                print("Valor inválido. Se usará 7 días.")
                ventanas = [7]
        else:
            valores = input("Ingrese los valores separados por coma (días) [180,365,730 recomendado]: ").strip()
            try:
                ventanas = [int(v.strip()) for v in valores.split(',') if v.strip()]
            except:
                print("Formato inválido. Se usarán [180, 365, 730].")
                ventanas = [180, 365, 730]
        
        tipo_espacial = 'k-rings' if es_h3 else 'radio'
        print(f"Generando características temporales con ventanas {ventanas} días y {tipo_espacial}={param_espacial if param_espacial is not None else 'sin espacial'}.")
        df_resultado = crear_features_temp_multiple(df, ventanas, param_espacial)
        return df_resultado
    
    elif opcion_tipo == '2':
        incluir_espacial = input("¿Incluir componente espacial? (s/n): ").strip().lower()
        espacial = incluir_espacial == 's'
        param_espacial = None

        if espacial:
            if es_h3:
                radio_input = input("Niveles de vecindad H3 (k-rings, 0 = solo el propio hexágono, por defecto 0): ").strip()
                param_espacial = int(radio_input) if radio_input else 0
            else:
                radio_input = input("Radio en km (por defecto 10): ").strip()
                param_espacial = float(radio_input) if radio_input else 10.0
        
        tipo_espacial = 'k-rings' if es_h3 else 'radio'
        print(f"Generando características estacionales con todos los años anteriores y {tipo_espacial}={param_espacial if param_espacial is not None else 'sin espacial'}.")
        df_resultado = crear_features_temp(df, param_espacial)
        return df_resultado
    
    elif opcion_tipo == '3':
        print("\nConfiguración TEMPORAL (días hacia atrás)")
        incluir_espacial_temp = input("¿Incluir componente espacial? (s/n): ").strip().lower()
        radio_temp = None

        if incluir_espacial_temp == 's':
            if es_h3:
                radio_input = input("Niveles de vecindad H3 (k-rings, por defecto 0): ").strip()
                radio_temp = int(radio_input) if radio_input else 0
            else:
                radio_input = input("Radio en km para parte temporal (por defecto 10): ").strip()
                radio_temp = float(radio_input) if radio_input else 10.0

        opcion_ventanas_temp = input("¿Una o más ventanas temporales? (1/más): ").strip()
        if opcion_ventanas_temp == '1':
            valor = input("Valor de la ventana temporal (días): ").strip()
            try:
                ventanas_temp = [int(valor)]
            except:
                print("Valor inválido. Se usará 7 días.")
                ventanas_temp = [7]
        else:
            valores = input("Ingrese los valores separados por coma (días) [180,365,730 recomendado]: ").strip()
            try:
                ventanas_temp = [int(v.strip()) for v in valores.split(',') if v.strip()]
            except:
                print("Formato inválido. Se usarán [180, 365, 730].")
                ventanas_temp = [180, 365, 730]
        
        print("\nConfiguración ESTACIONAL (mismo mes en todos los años anteriores)")
        incluir_espacial_est = input("¿Incluir componente espacial en la parte estacional? (s/n): ").strip().lower()
        radio_est = None

        if incluir_espacial_est == 's':
            if es_h3:
                radio_input = input("Niveles de vecindad H3 (k-rings, por defecto 0): ").strip()
                radio_est = int(radio_input) if radio_input else 0
            else:
                radio_input = input("Radio en km para parte estacional (por defecto 10): ").strip()
                radio_est = float(radio_input) if radio_input else 10.0

        print("\nGenerando características temporales y estacionales combinadas...")
        df_temp = crear_features_temp_multiple(df, ventanas_temp, radio_temp)
        df_combined = crear_features_temp(df_temp, radio_est)
        return df_combined
    
    else:
        print("Opción no válida. Intente nuevamente.")
        return menu_ventanas_temporales(df)
    

def ventanas():
    X, y = cargar_dataset_general_con_tiempos(eliminar_correladas=False)
    
    df_completo = pd.concat([X, y.rename('incendio')], axis=1)
    print(df_completo.columns.to_list())
    df_completo['date'] = pd.to_datetime(df_completo['date'])
    
    df_features = menu_ventanas_temporales(df_completo)
    
    for col in df_features.columns:
        if col.startswith('incendios_recientes_'):
            w = col.split('_')[-1]  
            df_features[f'hubo_incendio_{w}'] = (df_features[col] > 0).astype(int)
            dias = int(w.replace('d',''))
            df_features[f'frecuencia_incendios_{w}'] = df_features[col] / dias
            df_features[f'log_incendios_{w}'] = np.log1p(df_features[col])
        elif col.startswith('dias_ultimo_incendio_'):
            df_features[f'log_{col}'] = np.log1p(df_features[col].clip(lower=0))
    
    if 'incendios_estacionales' in df_features.columns:
        df_features['hubo_incendio_estacional'] = (df_features['incendios_estacionales'] > 0).astype(int)
        df_features['log_incendios_estacional'] = np.log1p(df_features['incendios_estacionales'])
    
    if 'dias_ultimo_incendio_estacional' in df_features.columns:
        df_features['log_dias_estacional'] = np.log1p(df_features['dias_ultimo_incendio_estacional'].clip(lower=0))
        
    y_features = df_features['incendio']
    X_features = df_features.drop(['incendio', 'date'], axis=1)
    
    print(f"Nulos en X_features antes de split: {X_features.isna().sum().sum()}")
    
    X_train, X_val, X_test, y_train, y_val, y_test = split_temporal(X_features, y_features)
    print(f"Tamaños: train={X_train.shape[0]}, val={X_val.shape[0]}, test={X_test.shape[0]}")
    print(f"Nulos en X_train después de procesar: {X_train.isna().sum().sum()}")

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

        config = {
            **model_params,
            "split": "temporal",
            "eliminar_correladas": False,
            "n_features": X_features.shape[1],
            "arquitectura": model_name,
            "umbral_utilizado": mejor_umbral
        }

        run = wandb.init(
            entity=WANDB_ENTITY,
            name=f"{model_name}",
            project=WANDB_PROJECT,
            config=config,
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
        ax.set_title(f'Matriz de Confusión Normalizada (Test)\n{model_name} - Umbral: {mejor_umbral:.2f}')        
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
    ventanas()