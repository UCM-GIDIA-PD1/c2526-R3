import pandas as pd
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import random
from sklearn.model_selection import train_test_split
from sklearn.metrics import fbeta_score, recall_score, f1_score, confusion_matrix
from imblearn.under_sampling import RandomUnderSampler
from extraccion import minioFunctions as mf
import wandb
from wandb.sklearn import plot_roc, plot_precision_recall, plot_feature_importances, plot_confusion_matrix

warnings.simplefilter(action='ignore', category=FutureWarning)

WANDB_PROJECT = "modelo_incremental"
SEED = 42
NUM_IT = 0

mejor_score_global = -1
mejores_hiperparametros_global = {}


def descargar_datos(path_server, target_col='final'):
    df_entero = mf.bajar_fichero(
        mf.crear_cliente(), 
        path_server=path_server, 
        type="df"
    )
    
    if 'date' in df_entero.columns:
        df_entero = df_entero.sort_values('date')
        
        df_entero['precip_7d'] = df_entero['precipitation'].rolling(window=7, min_periods=1).sum()
        df_entero['caida_humedad'] = df_entero['humidity_mean'].diff()
        df_entero['deficit_presion_vapor'] = df_entero['temp_mean'] * (100 - df_entero['humidity_mean'])
        df_entero['precip_14d'] = df_entero['precipitation'].rolling(window=14, min_periods=1).sum()
        df_entero['viento_sostenido_3d'] = df_entero['wind_speed_max'].rolling(window=3, min_periods=1).mean()
        df_entero['calor_sostenido_5d'] = df_entero['temp_mean'].rolling(window=5, min_periods=1).mean()
        df_entero['tendencia_temperatura_3d'] = df_entero['temp_mean'].diff(3)
        df_entero['cambio_brusco_viento'] = df_entero['wind_speed_max'].diff()

        if 'wind_speed_max' in df_entero.columns:
            df_entero['factor_30_30_30'] = (
                (df_entero['temp_mean'] > 30).astype(int) + \
                (df_entero['wind_speed_max'] > 30).astype(int) + \
                (df_entero['humidity_mean'] < 30).astype(int)
            )
            df_entero['riesgo_viento'] = df_entero['wind_speed_max'] / (df_entero['humidity_mean'] + 1)

        if 'soil_temp' in df_entero.columns:
            df_entero['calor_acumulado_suelo'] = df_entero['soil_temp'].rolling(window=3, min_periods=1).mean()

        if 'dist_civ' in df_entero.columns:
            df_entero['riesgo_antropico'] = df_entero['deficit_presion_vapor'] / (df_entero['dist_civ'] + 1)

        df_entero['indice_sequia_extrema'] = (df_entero['deficit_presion_vapor'] * df_entero['temp_mean']) / (df_entero['precip_14d'] + 1)

        if 'NDVI' in df_entero.columns:
            df_entero['estres_vegetacion'] = df_entero['NDVI'] / (df_entero['precip_14d'] + 0.1)

        for i in [1, 2]:
            df_entero[f'humidity_lag_{i}'] = df_entero['humidity_mean'].shift(i)
            df_entero[f'precip_sum_{i}d'] = df_entero['precipitation'].rolling(window=i+1, min_periods=1).sum()
    
    if 'temp_mean' in df_entero.columns and 'soil_temp' in df_entero.columns:
        df_entero['diff_aire_suelo'] = df_entero['temp_mean'] - df_entero['soil_temp']

    if 'riesgo_viento' in df_entero.columns and 'diff_aire_suelo' in df_entero.columns:
        df_entero['indice_ignicion'] = df_entero['riesgo_viento'] * df_entero['diff_aire_suelo']
    
    if 'humidity_mean' in df_entero.columns and 'humidity_lag_1' in df_entero.columns:
        df_entero['estres_hidrico_acum'] = (df_entero['humidity_mean'] + df_entero['humidity_lag_1']) / 2

    if 'radiation' in df_entero.columns and 'cloud_cover' in df_entero.columns:
        df_entero['energia_solar'] = df_entero['radiation'] * (1 - (df_entero['cloud_cover'] / 100))

    df_entero = df_entero.bfill().fillna(0)
    
    cols_fecha = df_entero.select_dtypes(include=['datetime', 'datetime64[ns]']).columns
    for col in cols_fecha:
        df_entero[f'{col}_mes'] = df_entero[col].dt.month
        df_entero[f'{col}_dia'] = df_entero[col].dt.day
        df_entero = df_entero.drop(columns=[col])
        
    if 'date' in df_entero.columns: 
        df_entero = df_entero.drop(columns=['date'])

    cols_predictoras = [col for col in df_entero.columns if col != target_col]
    df_entero[cols_predictoras] = df_entero[cols_predictoras].round(2)
    
    return df_entero


def dividir_datos(df_entero, target_col):
    X = df_entero.drop(columns=[target_col])
    y = df_entero[target_col]
    
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=0.2, random_state=SEED, stratify=y
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=0.25, random_state=SEED, stratify=y_temp
    )
    
    return X_train, X_val, X_test, y_train, y_val, y_test


def seleccionar_columnas(df, opcion, target_col):
    if opcion == 'all':
        return df
    elif opcion == 'no_date':
        cols_a_excluir = [col for col in df.columns if col.endswith('_mes') or col.endswith('_dia')]
        return df.drop(columns=cols_a_excluir)
    elif opcion == 'most_significant':
        cols_mantener = [
            'humidity_mean', 'soil_temp', 'riesgo_viento', 'diff_aire_suelo', 
            'NDVI', 'indice_ignicion', 'precip_7d', 'caida_humedad',
            'deficit_presion_vapor', 'dist_civ', 'riesgo_antropico', target_col
        ]
        cols_disponibles = [c for c in cols_mantener if c in df.columns]
        return df[cols_disponibles]
    elif opcion == 'super_features':
        cols_mantener = [
            'indice_sequia_extrema', 'estres_vegetacion', 'tendencia_temperatura_3d', 
            'cambio_brusco_viento', 'precip_14d', 'calor_sostenido_5d', 'factor_30_30_30',
            'indice_ignicion', 'riesgo_antropico', 'NDVI', 'dist_civ', target_col
        ]
        cols_disponibles = [c for c in cols_mantener if c in df.columns]
        return df[cols_disponibles]
    else:
        return df


def filtrar_datos(X_train, y_train, target_col, n_coincidencias, usar_quirurgico, vars_quir, proporcion_objetivo=0.5, feature_set='all'):
    df_train = pd.concat([X_train, y_train], axis=1)
    df_train = seleccionar_columnas(df_train, feature_set, target_col)
    
    cols_predictoras = [col for col in df_train.columns if col != target_col]
    df_positivos = df_train[df_train[target_col] == 1]
    df_negativos = df_train[df_train[target_col] == 0].copy()

    if usar_quirurgico:
        similares = pd.Series(False, index=df_negativos.index)
        for _, positivo in df_positivos.iterrows():
            mask_exactas = df_negativos[cols_predictoras].eq(positivo[cols_predictoras]).sum(axis=1) >= n_coincidencias
            mask_quir = pd.Series(False, index=df_negativos.index)
            for v in vars_quir:
                if v not in df_negativos.columns: continue
                tol = 3.0 if ('vapor' in v or 'sequia' in v or 'estres' in v) else 0.5
                mask_quir |= df_negativos[v].between(positivo[v] - tol, positivo[v] + tol)
            similares |= (mask_exactas | mask_quir)
        
        indices_similares = df_negativos[similares].index
        n_negativos_objetivo = int(len(df_positivos) * proporcion_objetivo)
        
        if len(indices_similares) > n_negativos_objetivo:
            keep_indices = np.random.choice(indices_similares, size=n_negativos_objetivo, replace=False)
            indices_a_mantener = list(set(df_negativos.index) - set(indices_similares)) + list(keep_indices)
            df_negativos = df_negativos.loc[indices_a_mantener]
        else:
            otros = df_negativos[~similares].index
            n_adicionales = max(0, n_negativos_objetivo - len(indices_similares))
            if n_adicionales > 0 and len(otros) > 0:
                extra = np.random.choice(otros, size=min(n_adicionales, len(otros)), replace=False)
                indices_a_mantener = list(indices_similares) + list(extra)
                df_negativos = df_negativos.loc[indices_a_mantener]
    else:
        rus = RandomUnderSampler(sampling_strategy=proporcion_objetivo, random_state=SEED)
        X_res, y_res = rus.fit_resample(df_train.drop(columns=[target_col]), df_train[target_col])
        return pd.concat([X_res, y_res], axis=1)

    return pd.concat([df_positivos, df_negativos]).sample(frac=1, random_state=SEED)


def optimizar_umbral(y_true, y_probs, beta):
    mejor_score, mejor_u, mejor_recall = -1, 0.5, -1
    for u in np.linspace(0.01, 0.80, 150): 
        preds = (y_probs >= u).astype(int)
        recall = recall_score(y_true, preds, zero_division=0)
        fbeta = fbeta_score(y_true, preds, beta=beta, zero_division=0)
        
        if recall >= 0.78:
            if fbeta > mejor_score:
                mejor_score, mejor_u, mejor_recall = fbeta, u, recall
        elif mejor_score == -1 and recall > mejor_recall:
            mejor_u, mejor_recall = u, recall
            
    return mejor_score, mejor_u


def inicializar():
    path_server = "grupo3/cleaned/MINI.parquet"
    target_col = 'final'
    beta = 2.0
    
    df_entero = descargar_datos(path_server, target_col)
    X_train, X_val, X_test, y_train, y_val, y_test = dividir_datos(df_entero, target_col)
    
    return {
        'X_train': X_train, 'y_train': y_train,
        'X_val': X_val, 'y_val': y_val,
        'X_test': X_test, 'y_test': y_test,
        'target_col': target_col,
        'beta': beta
    }


def entrenamiento(datos):
    global NUM_IT, mejor_score_global, mejores_hiperparametros_global
    NUM_IT += 1

    run = wandb.init(project=WANDB_PROJECT) 
    config = wandb.config

    mapa_vars = {
        "sequia": ['deficit_presion_vapor', 'precip_14d'],
        "sequia_extrema": ['indice_sequia_extrema', 'precip_14d'],
        "cambios_bruscos": ['tendencia_temperatura_3d', 'cambio_brusco_viento'],
        "viento": ['viento_sostenido_3d', 'riesgo_viento'],
        "vegetacion": ['estres_vegetacion', 'NDVI'],
        "antropico": ['dist_civ', 'riesgo_antropico'],
        "ignicion": ['indice_ignicion', 'calor_sostenido_5d']
    }
    vars_q = mapa_vars.get(config.vars_q_str, ['deficit_presion_vapor'])

    df_filtrado = filtrar_datos(
        datos['X_train'], datos['y_train'], datos['target_col'],
        n_coincidencias=config.n_coincidencias,
        usar_quirurgico=True,
        vars_quir=vars_q,
        proporcion_objetivo=config.prop,
        feature_set=config.feat
    )
    
    X_train_f = df_filtrado.drop(columns=[datos['target_col']])
    y_train_f = df_filtrado[datos['target_col']]

    params = {
        'objective': 'binary:logistic',
        'random_state': SEED,
        'n_estimators': config.n_estimators,
        'learning_rate': config.learning_rate,
        'max_depth': config.max_depth,
        'subsample': config.subsample,
        'colsample_bytree': config.colsample_bytree,
        'n_jobs': -1
    }
    
    if config.peso:
        params['scale_pos_weight'] = (y_train_f == 0).sum() / y_train_f.sum()

    clf = xgb.XGBClassifier(**params)
    clf.fit(X_train_f, y_train_f)

    X_val_f = seleccionar_columnas(datos['X_val'], config.feat, datos['target_col'])
    probs_val = clf.predict_proba(X_val_f)[:, 1]
    
    f2_val, u_val = optimizar_umbral(datos['y_val'], probs_val, datos['beta'])
    preds_val = (probs_val >= u_val).astype(int)
    
    f1_val = f1_score(datos['y_val'], preds_val, zero_division=0)
    recall_val = recall_score(datos['y_val'], preds_val, zero_division=0)
    
    cm = confusion_matrix(datos['y_val'], preds_val)
    if len(cm.ravel()) == 4:
        tn, fp, fn, tp = cm.ravel()
    else:
        tn, fp, fn, tp = 0, 0, 0, 0

    wandb.log({
        "val/f2_score": float(f2_val),
        "val/f1_score": float(f1_val),
        "val/recall": float(recall_val),
        "val/umbral_optimo": float(u_val),
        "val/tn": int(tn),
        "val/fp": int(fp),
        "val/fn": int(fn),
        "val/tp": int(tp)
    })

    if f2_val > mejor_score_global:
        mejor_score_global = f2_val
        mejores_hiperparametros_global = dict(config)
        mejores_hiperparametros_global['mejor_umbral'] = u_val

    probs_val_full = clf.predict_proba(X_val_f)
    plot_roc(datos['y_val'], probs_val_full)
    plot_precision_recall(datos['y_val'], probs_val_full)
    plot_confusion_matrix(datos['y_val'], preds_val, labels=["No Incendio", "Incendio"])
    
    if hasattr(clf, 'feature_importances_'):
        try:
            plot_feature_importances(clf)
        except Exception:
            pass

    run.finish()


def clasificacion_incremental(opcion, metodo_elegido, metrica, iteraciones):
    datos_completos = inicializar()

    def ent():
        entrenamiento(datos_completos)

    if opcion == "1":
        params = {
            "prop": {"values": [0.1, 0.2, 0.3]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.1, "max": 0.3},
            "feat": {"values": ["super_features", "most_significant"]},
            "vars_q_str": {"values": ["ignicion", "sequia_extrema"]}, 
            "n_coincidencias": {"values": [4, 6]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 4, "max": 6},
            "n_estimators": {"values": [200, 300]},
            "learning_rate": {"values": [0.01, 0.03]},
            "max_depth": {"values": [6, 8]},
            "subsample": {"values": [0.8]},
            "colsample_bytree": {"values": [0.8]},
            "peso": {"values": [True, False]}
        }
    elif opcion == "2":
        params = {
            "prop": {"values": [0.4, 0.6, 0.8]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.4, "max": 0.8},
            "feat": {"values": ["super_features", "all"]},
            "vars_q_str": {"values": ["cambios_bruscos", "vegetacion"]}, 
            "n_coincidencias": {"values": [6, 8]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 6, "max": 8},
            "n_estimators": {"values": [300, 400]},
            "learning_rate": {"values": [0.02, 0.04]},
            "max_depth": {"values": [7, 9]},
            "subsample": {"values": [0.85]},
            "colsample_bytree": {"values": [0.85]},
            "peso": {"values": [True]}
        }
    elif opcion == "3":
        params = {
            "prop": {"values": [1.0, 1.25, 1.5]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 1.0, "max": 1.5},
            "feat": {"values": ["super_features", "all"]},
            "vars_q_str": {"values": ["sequia_extrema", "antropico"]},
            "n_coincidencias": {"values": [8, 10]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 8, "max": 10},
            "n_estimators": {"values": [400, 500]},
            "learning_rate": {"values": [0.03, 0.05]},
            "max_depth": {"values": [8, 10]},
            "subsample": {"values": [0.9]},
            "colsample_bytree": {"values": [0.9]},
            "peso": {"values": [True, False]}
        }
    elif opcion == "4":
        params = {
            "prop": {"values": [0.1, 0.3, 0.5, 0.8, 1.0, 1.5]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.1, "max": 1.5},
            "feat": {"values": ["super_features", "most_significant", "all"]},
            "vars_q_str": {"values": ["sequia_extrema", "cambios_bruscos", "vegetacion", "ignicion"]},
            "n_coincidencias": {"values": [4, 6, 8, 10]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 4, "max": 10},
            "n_estimators": {"values": [250, 400, 500]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 250, "max": 500},
            "learning_rate": {"values": [0.01, 0.03, 0.05]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.01, "max": 0.05},
            "max_depth": {"values": [6, 8, 10]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 6, "max": 10},
            "subsample": {"values": [0.8, 1.0]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.8, "max": 1.0},
            "colsample_bytree": {"values": [0.8, 1.0]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 0.8, "max": 1.0},
            "peso": {"values": [True, False]}
        }

    metric_name = "val/f2_score" if metrica == "f2" else "val/f1_score"

    sweep_config = {
        "name": f"Incremental-Fase{opcion}-{metodo_elegido}-{metrica}",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": "maximize"},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, project=WANDB_PROJECT)

    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iteraciones,
        project=WANDB_PROJECT,
    )

    if mejores_hiperparametros_global:
        df_mejores = pd.DataFrame([mejores_hiperparametros_global])
        df_mejores.to_csv(f"mejores_hiperparametros_incremental_fase_{opcion}.csv", index=False)
        print(f"\n CSV guardado con éxito.")


if __name__ == "__main__":
    print("\nMODELO INCREMENTAL")
    print("1. FASE 1: Alta Sensibilidad (Prop 0.1 - 0.3)")
    print("2. FASE 2: Inyección de Ruido (Prop 0.4 - 0.8)")
    print("3. FASE 3: Entorno Real (Prop 1.0 - 1.5)")
    print("4. MODO ESCALADA COMPLETA (Prop 0.1 a 1.5)")
    
    opcion = input("\nElige una fase a explorar (1-4): ")
    
    if opcion in ["1", "2", "3", "4"]:
        metrica = input("Selecciona la métrica que quieres optimizar (f1/f2): ")
        metodo = input("Selecciona el metodo (grid, random o bayes): ")
        iteraciones = int(input("Introduce el número máximo de iteraciones: "))
        clasificacion_incremental(opcion, metodo, metrica, iteraciones)
    else:
        print("Opción no válida.")