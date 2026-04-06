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

WANDB_PROJECT = "modelo_inversa"
SEED = 42
NUM_IT = 0

mejor_score_global = -1
mejores_hiperparametros_global = {}

class ModeloFiltroIncendios:
    def __init__(self, path_server, target_col='final', beta=2.0):
        self.path_server = path_server
        self.target_col = target_col
        self.beta = beta
        self.df_entero = None
        self.X_train = None
        self.y_train = None
        self.X_val = None
        self.y_val = None
        self.X_test = None
        self.y_test = None

    def descargar_datos(self):
        self.df_entero = mf.bajar_fichero(
            mf.crear_cliente(), 
            path_server=self.path_server, 
            type="df"
        )
        
        if 'date' in self.df_entero.columns:
            self.df_entero = self.df_entero.sort_values('date')
            
            self.df_entero['precip_7d'] = self.df_entero['precipitation'].rolling(window=7).sum()
            self.df_entero['caida_humedad'] = self.df_entero['humidity_mean'].diff()
            
            self.df_entero['deficit_presion_vapor'] = self.df_entero['temp_mean'] * (100 - self.df_entero['humidity_mean'])

            if 'wind_speed_max' in self.df_entero.columns:
                self.df_entero['factor_30_30_30'] = (
                    (self.df_entero['temp_mean'] > 30).astype(int) + \
                    (self.df_entero['wind_speed_max'] > 30).astype(int) + \
                    (self.df_entero['humidity_mean'] < 30).astype(int)
                )

            if 'soil_temp' in self.df_entero.columns:
                self.df_entero['calor_acumulado_suelo'] = self.df_entero['soil_temp'].rolling(window=3).mean()

            if 'dist_civ' in self.df_entero.columns:
                self.df_entero['riesgo_antropico'] = self.df_entero['deficit_presion_vapor'] / (self.df_entero['dist_civ'] + 1)

            for i in [1, 2]:
                self.df_entero[f'humidity_lag_{i}'] = self.df_entero['humidity_mean'].shift(i)
                self.df_entero[f'precip_sum_{i}d'] = self.df_entero['precipitation'].rolling(window=i+1).sum()
        
        if 'temp_mean' in self.df_entero.columns and 'soil_temp' in self.df_entero.columns:
            self.df_entero['diff_aire_suelo'] = self.df_entero['temp_mean'] - self.df_entero['soil_temp']
            
        if 'wind_speed_max' in self.df_entero.columns:
            self.df_entero['riesgo_viento'] = self.df_entero['wind_speed_max'] / (self.df_entero['humidity_mean'] + 1)

        if 'riesgo_viento' in self.df_entero.columns and 'diff_aire_suelo' in self.df_entero.columns:
            self.df_entero['indice_ignicion'] = self.df_entero['riesgo_viento'] * self.df_entero['diff_aire_suelo']
        
        if 'humidity_mean' in self.df_entero.columns and 'humidity_lag_1' in self.df_entero.columns:
            self.df_entero['estres_hidrico_acum'] = (self.df_entero['humidity_mean'] + self.df_entero['humidity_lag_1']) / 2

        if 'radiation' in self.df_entero.columns and 'cloud_cover' in self.df_entero.columns:
            self.df_entero['energia_solar'] = self.df_entero['radiation'] * (1 - (self.df_entero['cloud_cover'] / 100))

        self.df_entero = self.df_entero.bfill().fillna(0)
        
        cols_fecha = self.df_entero.select_dtypes(include=['datetime', 'datetime64[ns]']).columns
        for col in cols_fecha:
            self.df_entero[f'{col}_mes'] = self.df_entero[col].dt.month
            self.df_entero[f'{col}_dia'] = self.df_entero[col].dt.day
            self.df_entero = self.df_entero.drop(columns=[col])
            
        if 'date' in self.df_entero.columns: 
            self.df_entero = self.df_entero.drop(columns=['date'])

        cols_predictoras = [col for col in self.df_entero.columns if col != self.target_col]
        self.df_entero[cols_predictoras] = self.df_entero[cols_predictoras].round(1)

    def dividir_datos(self):
        X = self.df_entero.drop(columns=[self.target_col])
        y = self.df_entero[self.target_col]
        X_temp, self.X_test, y_temp, self.y_test = train_test_split(
            X, y, test_size=0.2, random_state=SEED, stratify=y
        )
        self.X_train, self.X_val, self.y_train, self.y_val = train_test_split(
            X_temp, y_temp, test_size=0.25, random_state=SEED, stratify=y_temp
        )

    def seleccionar_columnas(self, df, opcion):
        if opcion == 'all':
            return df
        elif opcion == 'no_date':
            cols_a_excluir = [col for col in df.columns if col.endswith('_mes') or col.endswith('_dia')]
            return df.drop(columns=cols_a_excluir)
        elif opcion == 'most_significant':
            cols_mantener = [
                'humidity_mean', 'soil_temp', 'riesgo_viento', 'diff_aire_suelo', 
                'NDVI', 'indice_ignicion', 'precip_7d', 'caida_humedad',
                'deficit_presion_vapor', 'dist_civ', 'riesgo_antropico', self.target_col
            ]
            cols_disponibles = [c for c in cols_mantener if c in df.columns]
            return df[cols_disponibles]
        else:
            return df

    def filtrar_datos(self, n_coincidencias, usar_quirurgico, vars_quir, proporcion_objetivo=0.5, feature_set='all'):
        df_train = pd.concat([self.X_train, self.y_train], axis=1)
        df_train = self.seleccionar_columnas(df_train, feature_set)
        
        cols_predictoras = [col for col in df_train.columns if col != self.target_col]
        df_positivos = df_train[df_train[self.target_col] == 1]
        df_negativos = df_train[df_train[self.target_col] == 0].copy()

        if usar_quirurgico:
            similares = pd.Series(False, index=df_negativos.index)
            for _, positivo in df_positivos.iterrows():
                mask_exactas = df_negativos[cols_predictoras].eq(positivo[cols_predictoras]).sum(axis=1) >= n_coincidencias
                mask_quir = pd.Series(False, index=df_negativos.index)
                for v in vars_quir:
                    if v not in df_negativos.columns: continue
                    tol = 2.0 if 'dist' in v or 'vapor' in v else 0.5
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
            X_res, y_res = rus.fit_resample(df_train.drop(columns=[self.target_col]), df_train[self.target_col])
            return pd.concat([X_res, y_res], axis=1)

        return pd.concat([df_positivos, df_negativos]).sample(frac=1, random_state=SEED)

    def optimizar_umbral(self, y_true, y_probs):
        mejor_score, mejor_u, mejor_recall = -1, 0.5, -1
        for u in np.linspace(0.01, 0.80, 150): 
            preds = (y_probs >= u).astype(int)
            recall = recall_score(y_true, preds, zero_division=0)
            fbeta = fbeta_score(y_true, preds, beta=self.beta, zero_division=0)
            
            if recall >= 0.78:
                if fbeta > mejor_score:
                    mejor_score, mejor_u, mejor_recall = fbeta, u, recall
            elif mejor_score == -1 and recall > mejor_recall:
                mejor_u, mejor_recall = u, recall
                
        return mejor_score, mejor_u


def inicializar():
    modelo_obj = ModeloFiltroIncendios(path_server="grupo3/cleaned/MINI.parquet")
    modelo_obj.descargar_datos()
    modelo_obj.dividir_datos()
    return modelo_obj


def entrenamiento(modelo_obj):
    global NUM_IT, mejor_score_global, mejores_hiperparametros_global
    NUM_IT += 1

    run = wandb.init(project=WANDB_PROJECT) 
    config = wandb.config

    mapa_vars = {
        "sequia": ['deficit_presion_vapor'],
        "viento": ['deficit_presion_vapor', 'riesgo_viento'],
        "humedad": ['humidity_mean', 'riesgo_viento'],
        "humedad_suelo": ['humidity_mean', 'soil_temp'],
        "antropico": ['dist_civ', 'deficit_presion_vapor'],
        "solo_dist_civ": ['dist_civ'],
        "riesgo_antropico": ['riesgo_antropico'],
        "ignicion": ['indice_ignicion', 'soil_temp']
    }
    vars_q = mapa_vars.get(config.vars_q_str, ['deficit_presion_vapor'])

    df_filtrado = modelo_obj.filtrar_datos(
        n_coincidencias=config.n_coincidencias,
        usar_quirurgico=True,
        vars_quir=vars_q,
        proporcion_objetivo=config.prop,
        feature_set=config.feat
    )
    
    X_train_f = df_filtrado.drop(columns=[modelo_obj.target_col])
    y_train_f = df_filtrado[modelo_obj.target_col]

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

    X_val_f = modelo_obj.seleccionar_columnas(modelo_obj.X_val, config.feat)
    probs_val = clf.predict_proba(X_val_f)[:, 1]
    
    f2_val, u_val = modelo_obj.optimizar_umbral(modelo_obj.y_val, probs_val)
    preds_val = (probs_val >= u_val).astype(int)
    
    f1_val = f1_score(modelo_obj.y_val, preds_val, zero_division=0)
    recall_val = recall_score(modelo_obj.y_val, preds_val, zero_division=0)
    
    cm = confusion_matrix(modelo_obj.y_val, preds_val)
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
    plot_roc(modelo_obj.y_val, probs_val_full)
    plot_precision_recall(modelo_obj.y_val, probs_val_full)
    plot_confusion_matrix(modelo_obj.y_val, preds_val, labels=["No Incendio", "Incendio"])
    
    if hasattr(clf, 'feature_importances_'):
        try:
            plot_feature_importances(clf)
        except Exception:
            pass

    run.finish()


def clasificacion(opcion, metodo_elegido, metrica, iteraciones):
    modelo_obj = inicializar()

    def ent():
        entrenamiento(modelo_obj)

    # Replicando exactamente las lógicas y variables de tu código original
    if opcion == "1":
        # Búsqueda Exhaustiva (Grid clásico de tu código)
        params = {
            "prop": {"values": [0.4, 0.6]},
            "feat": {"values": ["all", "no_date"]},
            "vars_q_str": {"values": ["humedad_suelo", "ignicion"]},
            "n_coincidencias": {"values": [6, 8, 10]},
            "n_estimators": {"values": [250]},
            "learning_rate": {"values": [0.03]},
            "max_depth": {"values": [7]},
            "subsample": {"values": [0.8]},
            "colsample_bytree": {"values": [0.8]},
            "peso": {"values": [True]}
        }
    elif opcion == "2":
        # Prueba Manual (Refinada Existente)
        params = {
            "prop": {"values": [0.4]},
            "feat": {"values": ["all"]},
            "vars_q_str": {"values": ["ignicion"]},
            "n_coincidencias": {"values": [8]},
            "n_estimators": {"values": [350]},
            "learning_rate": {"values": [0.02]},
            "max_depth": {"values": [8]},
            "subsample": {"values": [0.8]},
            "colsample_bytree": {"values": [0.8]},
            "peso": {"values": [True]}
        }
    elif opcion == "3":
        # Prueba Manual (Configuración Sequía VPD)
        params = {
            "prop": {"values": [10.0]},
            "feat": {"values": ["most_significant"]},
            "vars_q_str": {"values": ["sequia"]},
            "n_coincidencias": {"values": [2]},
            "n_estimators": {"values": [350]},
            "learning_rate": {"values": [0.02]},
            "max_depth": {"values": [8]},
            "subsample": {"values": [0.8]},
            "colsample_bytree": {"values": [0.8]},
            "peso": {"values": [True]}
        }
    elif opcion == "4":
        # Modo Explorador Automático (Genético)
        params = {
            "prop": {"values": [5.0, 8.0, 12.0, 15.0]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 5.0, "max": 15.0},
            "feat": {"values": ["all", "no_date", "most_significant"]},
            "vars_q_str": {"values": ["sequia", "viento", "humedad"]},
            "n_coincidencias": {"values": [3, 5, 7]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 3, "max": 7},
            "n_estimators": {"values": [350]},
            "learning_rate": {"values": [0.03]},
            "max_depth": {"values": [9]},
            "subsample": {"values": [1.0]},
            "colsample_bytree": {"values": [1.0]},
            "peso": {"values": [True]}
        }
    elif opcion == "5":
        # Modo Explorador Antrópico
        params = {
            "prop": {"values": [3.0, 6.0, 9.0, 12.0]} if metodo_elegido == "grid" else {"distribution": "uniform", "min": 3.0, "max": 12.0},
            "feat": {"values": ["most_significant", "all"]},
            "vars_q_str": {"values": ["solo_dist_civ", "antropico", "riesgo_antropico"]},
            "n_coincidencias": {"values": [2, 3, 5]} if metodo_elegido == "grid" else {"distribution": "int_uniform", "min": 2, "max": 5},
            "n_estimators": {"values": [400]},
            "learning_rate": {"values": [0.02]},
            "max_depth": {"values": [10]},
            "subsample": {"values": [1.0]},
            "colsample_bytree": {"values": [1.0]},
            "peso": {"values": [True]}
        }

    metric_name = "val/f2_score" if metrica == "f2" else "val/f1_score"

    sweep_config = {
        "name": f"XGB-Opcion{opcion}-{metodo_elegido}-{metrica}-Sweep",
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
        df_mejores.to_csv(f"mejores_hiperparametros_opcion_{opcion}.csv", index=False)
        print(f"\n CSV guardado con éxito. ¡Exploración terminada!")


if __name__ == "__main__":
    print("1. Ejecutar Búsqueda Exhaustiva (Grid Sweep)")
    print("2. Probar configuración Refinada (Existente)")
    print("3. Probar CONFIGURACIÓN SEQUÍA (VPD)")
    print("4. MODO EXPLORADOR AUTOMÁTICO (Combinaciones aleatorias/bayes)")
    print("5. MODO EXPLORADOR ANTRÓPICO (Civilización + Clima)")
    opcion = input("\nElige una opción (1-5): ")
    
    if opcion in ["1", "4", "5"]:
        metrica = input("Selecciona la métrica que quieres optimizar (f1/f2): ")
        if opcion == "1":
            metodo = "grid" 
            print("Método fijado en: grid (Búsqueda Exhaustiva)")
        else:
            metodo = input("Selecciona el metodo (grid, random o bayes) para la búsqueda: ")
        
        iteraciones = int(input("Introduce el número máximo de iteraciones: "))
        clasificacion(opcion, metodo, metrica, iteraciones)
        
    elif opcion in ["2", "3"]:
        print("\nEjecutando configuración manual predefinida (1 sola iteración en WandB)...")
        clasificacion(opcion, "grid", "f2", iteraciones=1)
    else:
        print("Opción no válida.")