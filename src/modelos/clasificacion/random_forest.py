import os
import sys
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.impute import SimpleImputer
from sklearn.metrics import fbeta_score, recall_score, f1_score

import wandb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

from modelos.utils.carga_datos import cargar_dataset_general, cargar_dataset_general_con_tiempos
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import RandomForestClassifier
import modelos.clasificacion.ventanas_temporales as ventana

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "sweep_random_forest_umbral_smote"
SWEEP_PATH = Path(__file__).with_name("randomforest_sweep.yaml")
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
        "method": metodo,
        "parameters": {
            "max_depth": {"values": [hiperparametros["max_depth"]]},
            "n_estimators": {"values": [hiperparametros["n_estimators"]]},
            "min_samples_split": {"values": [hiperparametros["min_samples_split"]]},
            "min_samples_leaf": {"values": [hiperparametros["min_samples_leaf"]]},
            "criterion": {"values": [hiperparametros["criterion"]]},
            "max_features": {"values": [hiperparametros["max_features"]]},
            "umbral": {"values": [hiperparametros["umbral"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):

    columnas_validas = X_train_full.columns[X_train_full.notna().any()].tolist()
    
    X_train_limpio = X_train_full[columnas_validas]
    X_test_limpio = X_test[columnas_validas]

    run = wandb.init(tags=["Evaluacion Final", metodo]) 
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    clf = RandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    imputer = SimpleImputer(strategy='median')
    X_train_imputed = pd.DataFrame(imputer.fit_transform(X_train_limpio), columns=columnas_validas)
    X_test_imputed = pd.DataFrame(imputer.transform(X_test_limpio), columns=columnas_validas)

    smote = SMOTE(random_state=SEED)
    X_train_res, y_train_res = smote.fit_resample(X_train_imputed, y_train_full)

    clf.fit(X_train_res, y_train_res)

    y_prob_test = clf.predict_proba(X_test_imputed)
    y_pred_test = (y_prob_test[:, 1] >= config.umbral).astype(int)
    
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — RandomForest")

    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/precision": float(metricas_test["precision"]),
        "test/recall": float(metricas_test["recall"]),
        "test/accuracy": float(metricas_test["accuracy"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0),
        "test/f1_score": fbeta_score(y_test, y_pred_test, zero_division=0)
    })

    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    wandb.sklearn.plot_classifier(
        clf,
        X_train_imputed,
        X_test_imputed,
        y_train_full,
        y_test,
        y_pred_test,
        y_prob_test,
        labels=["no_incendio", "incendio"],
        model_name="RandomForest",
        feature_names=columnas_validas,
    )
    
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, columnas_validas)

    run.finish()

def entrenamiento(X_train_full, y_train_full, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    max_f = None if config.max_features == "None" else config.max_features

    clf = RandomForestClassifier(
        max_depth=config.max_depth,
        n_estimators=config.n_estimators,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        random_state=SEED,
        n_jobs=-1,
    )

    columnas_con_datos = X_train_full.columns[X_train_full.notna().any()].tolist()

    X_train_limpio = X_train_full[columnas_con_datos]

    imputer = SimpleImputer(strategy='median')
    
    X_train_full_imputed = pd.DataFrame(imputer.fit_transform(X_train_limpio), 
                                         columns=X_train_limpio.columns)
    
    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    smote = SMOTE(random_state=SEED)

    f2_cv_scores, f1_cv_scores = [], []
    f2_cv_train, f1_cv_train = [], []
    tns, fps, fns, tps = [], [], [], []

    smote = SMOTE(random_state=SEED)

    for train_idx, val_idx in cv_generator.split(X_train_full_imputed, y_train_full):

        X_fold_train = X_train_full_imputed.iloc[train_idx]
        X_fold_val = X_train_full_imputed.iloc[val_idx]

        y_fold_train, y_fold_val = y_train_full.iloc[train_idx], y_train_full.iloc[val_idx]

        X_fold_train_res, y_fold_train_res = smote.fit_resample(X_fold_train, y_fold_train)
        
        clf.fit(X_fold_train_res, y_fold_train_res)
        
        y_val_prob = clf.predict_proba(X_fold_val)[:, 1]
        y_val_pred = (y_val_prob >= config.umbral).astype(int)
    
        y_t_prob = clf.predict_proba(X_fold_train_res)[:, 1]
        y_t_pred = (y_t_prob >= config.umbral).astype(int)
        f2_cv_train.append(fbeta_score(y_fold_train_res, y_t_pred, beta=2, zero_division=0))
        f1_cv_train.append(fbeta_score(y_fold_train_res, y_t_pred, beta = 1, zero_division=0))

        f2_cv_scores.append(fbeta_score(y_fold_val, y_val_pred, beta=2, zero_division=0))
        f1_cv_scores.append(fbeta_score(y_fold_val, y_val_pred, beta = 1, zero_division=0))

        cm = confusion_matrix(y_fold_val, y_val_pred)

        tns.append(cm[0,0])
        fps.append(cm[0,1])
        fns.append(cm[1,0])
        tps.append(cm[1,1])

    wandb.log({
        "train/f2_mean_cv": np.mean(f2_cv_train),
        "train/f1_mean_cv": np.mean(f1_cv_train),
        "val/f2_mean_cv": np.mean(f2_cv_scores), 
        "val/f1_mean_cv": np.mean(f1_cv_scores),
        "val/tn_mean": np.mean(tns),
        "val/fp_mean": np.mean(fps),
        "val/fn_mean": np.mean(fns),
        "val/tp_mean": np.mean(tps)
    })

    X_train_res, y_train_res = smote.fit_resample(X_train_full_imputed, y_train_full)
    clf.fit(X_train_res, y_train_res)

    plot_feature_importances(clf)
    
    y_pred_train = (clf.predict_proba(X_train_full_imputed)[:, 1] >= config.umbral).astype(int)
    
    wf.matriz_confusion_feature_importance(clf, y_pred_train, y_train_full, columnas_con_datos)

    run.finish()


def inicializar():
    
    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cargar_dataset_general_con_tiempos(eliminar_correladas=False)

    df_completo = pd.concat([X, y.rename('incendio')], axis=1)
    df_completo['date'] = pd.to_datetime(df_completo['date'])
    
    df_features = ventana.menu_ventanas_temporales(df_completo)
    
    print("\n--- Post-procesando variables generadas (Logs y Hubo Incendio) ---")
    for col in df_features.columns:
        if col.startswith('incendios_recientes_'):
            w = col.split('_')[-1]  
            df_features[f'hubo_incendio_{w}'] = (df_features[col] > 0).astype(int)
            dias = int(w.replace('d',''))
            df_features[f'frecuencia_incendios_{w}'] = df_features[col] / dias
            df_features[f'log_{col}'] = np.log1p(df_features[col])
        elif col.startswith('dias_ultimo_incendio_'):
            df_features[f'log_{col}'] = np.log1p(df_features[col].clip(lower=0))
    
    if 'incendios_estacionales' in df_features.columns:
        df_features['hubo_incendio_estacional'] = (df_features['incendios_estacionales'] > 0).astype(int)
        df_features['log_incendios_estacional'] = np.log1p(df_features['incendios_estacionales'])
    
    if 'dias_ultimo_incendio_estacional' in df_features.columns:
        df_features['log_dias_estacional'] = np.log1p(df_features['dias_ultimo_incendio_estacional'].clip(lower=0))

    y_final = df_features['incendio']
    
    if "id_hexagono" in df_features.columns:
        df_features = df_features.drop(columns=['id_hexagono'])
        
    X_final = df_features.drop(['incendio', 'date'], axis=1, errors='ignore')

    df_para_limpieza = pd.concat([X_final, y_final.rename('incendio')], axis=1)
    X_final, y_final = pers.pregunta_PCA(df=df_para_limpieza)

    X_train_full, X_test, y_train_full, y_test = split_temporal(X_final, y_final, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test


def clasificacion(metodo_elegido, metrica_elegida):
    X_train_full, X_test, y_train_full, y_test = inicializar()
  
    iters, nombre = pers.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train_full, y_train_full, nombre)

    if metodo_elegido == "grid":

        params = {
            "n_estimators": {"values": [100, 500, 900]},
            "max_depth": {"values": [10, 20, 30]},
            "min_samples_leaf": {"values": [1, 10]},
            "min_samples_split": {"values": [2, 10]},
            "umbral": {"values": [0.3, 0.4, 0.5]},
            "criterion": {"values": ["gini", "entropy"]},
            "max_features": {"values": ["sqrt", None]}
        }

    elif metodo_elegido == "random":

        params = {
            "n_estimators": {"distribution": "int_uniform", "min": 10, "max": 1200},
            "max_depth": {"distribution": "int_uniform", "min": 2, "max": 50},
            "min_samples_leaf": {"distribution": "int_uniform", "min": 1, "max": 100},
            "min_samples_split": {"distribution": "int_uniform", "min": 2, "max": 40},
            "umbral": {"distribution": "uniform", "min": 0.05, "max": 0.9},
            "criterion": {"values": ["gini", "entropy"]},
            "max_features": {"values": ["sqrt", "log2", None]}
        }

    elif metodo_elegido == "bayes":
       
        params = {
            "n_estimators": {"distribution": "int_uniform", "min": 100, "max": 800},
            "max_depth": {"distribution": "int_uniform", "min": 5, "max": 30},
            "min_samples_leaf": {"distribution": "int_uniform", "min": 2, "max": 20},
            "min_samples_split": {"distribution": "int_uniform", "min": 2, "max": 20},
            "umbral": {"distribution": "uniform", "min": 0.2, "max": 0.6}, 
            "criterion": {"values": ["gini", "entropy"]},
            "max_features": {"values": ["sqrt", None]}
        }

    metrica_limpia = metrica_elegida.lower().strip()
    if "f2" in metrica_limpia:
        metric_name = "val/f2_mean_cv"
    else:
        metric_name = "val/f1_mean_cv"

    sweep_config = {
        "name": f"RF-{metodo_elegido}-{metrica_elegida}-Sweep",
        "method": metodo_elegido, 
        "metric": {"name": metric_name, "goal": "maximize"},
        "parameters": params
    }
    sweep_id = wandb.sweep(
        sweep_config, 
        entity=WANDB_ENTITY, 
        project="sweep_random_forest_umbral_smote"
    )

    wandb.agent(
        sweep_id=sweep_id,
        function=ent,
        count=iters
    )

if __name__ == "__main__":
    metodo = input("\n Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros:" )
    metrica = input("\n Selecciona la métrica que quieres optimizar (f1/f2):" )
    clasificacion(metodo, metrica)
