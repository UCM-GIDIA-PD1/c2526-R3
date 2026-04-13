import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from sklearn.metrics import fbeta_score, recall_score, f1_score

import wandb
import xgboost as xgb
from wandb.sklearn import (
    plot_roc,
    plot_precision_recall,
    plot_feature_importances,
)

# Imports de tus módulos locales
from modelos.utils.carga_datos import cargar_dataset_general_con_tiempos
from modelos.utils.particiones import split_temporal
from modelos.utils.metricas import evaluar_clasificacion
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers
import modelos.clasificacion.ventanas_temporales as ventana

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SEED = 42

def calcular_ratio_clases(y):
    counts = y.value_counts()
    return counts[0] / counts[1]

def evaluacion_final(config, X_train_full, X_test, y_train_full, y_test, metodo):
    """
    Entrena el modelo final con los parámetros dados y registra en W&B.
    """
    run = wandb.init(
        project=WANDB_PROJECT, 
        entity=WANDB_ENTITY, 
        name=f"Run-{metodo}", 
        tags=["Manual", metodo],
        config=config,
        reinit=True 
    )

    ratio = calcular_ratio_clases(y_train_full)
    
    # Construcción del modelo con los parámetros de tu JSON
    clf = xgb.XGBClassifier(
        n_estimators=int(config.get("n_estimators", 1000)),
        learning_rate=config.get("learning_rate", 0.1),
        max_depth=config.get("max_depth", 6), 
        subsample=config.get("subsample", 1.0),
        colsample_bytree=config.get("colsample_bytree", 1.0),
        min_child_weight=config.get("min_child_weight", 1),
        gamma=config.get("gamma", 0),
        scale_pos_weight=ratio,
        random_state=config.get("random_state", 42),
        eval_metric=config.get("eval_metric", "aucpr"),
        objective=config.get("objective", "binary:logistic"),
        enable_categorical=config.get("enable_categorical", False),
        n_jobs=-1,
    )

    print(f"\n--- Entrenando modelo con {config.get('n_estimators')} estimadores ---")
    clf.fit(X_train_full, y_train_full)

    # Buscar mejor umbral en train para aplicar en test
    y_prob_train = clf.predict_proba(X_train_full)[:, 1]
    umbral_optimo = ventana.encontrar_mejor_umbral(y_train_full, y_prob_train)
    print(f"Umbral óptimo calculado: {umbral_optimo:.4f}")

    # Predicciones en Test
    y_prob_test = clf.predict_proba(X_test)
    y_pred_test = (y_prob_test[:, 1] >= umbral_optimo).astype(int)

    # Evaluación y Logs
    metricas_test = evaluar_clasificacion(y_test, y_pred_test, y_prob_test[:, 1], "Test — XGBoost")
    
    wandb.log({
        "test/f1": float(metricas_test["f1"]),
        "test/recall": float(metricas_test["recall"]),
        "test/f2_score": fbeta_score(y_test, y_pred_test, beta=2, zero_division=0),
        "umbral_utilizado": umbral_optimo
    })

    # Gráficos automáticos
    plot_roc(y_test, y_prob_test)
    plot_precision_recall(y_test, y_prob_test)
    plot_feature_importances(clf)
    wf.matriz_confusion_feature_importance(clf, y_pred_test, y_test, X_train_full.columns.tolist())

    run.finish()
    return clf

def entrenar_con_parametros_fijos(X_train_full, X_test, y_train_full, y_test, params_fijos):
    """
    Función orquestadora: Entrena, Evalúa y descarga el PKL.
    """
    print("\n--- Iniciando entrenamiento individual con parámetros fijos ---")
    
    # 1. Ejecutar entrenamiento y evaluación en W&B
    modelo_entrenado = evaluacion_final(params_fijos, X_train_full, X_test, y_train_full, y_test, "Fijos-JSON")
    
    # 2. Guardar el modelo localmente
    nombre_pkl = "modelo_xgboost_final.pkl"
    with open(nombre_pkl, "wb") as f:
        pickle.dump(modelo_entrenado, f)
        
    print(f"\n✅ Proceso completado exitosamente.")
    print(f"✅ Modelo exportado a: {nombre_pkl}")

def inicializar():
    """
    Carga y preprocesamiento de datos.
    """
    if not wf.inicializar_apikey_wandb():
        return None, None, None, None
    
    X, y = cargar_dataset_general_con_tiempos(eliminar_correladas=False)
    df_completo = pd.concat([X, y.rename('incendio')], axis=1)
    df_completo['date'] = pd.to_datetime(df_completo['date'])
    
    df_features = ventana.menu_ventanas_temporales(df_completo)
    
    for col in df_features.columns:
        if col.startswith('incendios_recientes_'):
            w = col.split('_')[-1]  
            df_features[f'hubo_incendio_{w}'] = (df_features[col] > 0).astype(int)
            dias = int(w.replace('d',''))
            df_features[f'frecuencia_incendios_{w}'] = df_features[col] / dias
            df_features[f'log_{col}'] = np.log1p(df_features[col])
        elif col.startswith('dias_ultimo_incendio_'):
            df_features[f'log_{col}'] = np.log1p(df_features[col].clip(lower=0))

    y_final = df_features['incendio']
    X_final = df_features.drop(['incendio', 'date'], axis=1, errors='ignore')

    X_train_full, X_test, y_train_full, y_test = split_temporal(X_final, y_final, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)

    return X_train_full, X_test, y_train_full, y_test

if __name__ == "__main__":
    
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    if X_train_full is not None:

        mis_params = {
            "n_estimators": 10000,
            "learning_rate": 0.1,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "random_state": 42,
            "max_depth": 6,       
            "subsample": 1.0,     
            "colsample_bytree": 1.0, 
            "gamma": 0,
            "min_child_weight": 1,
            "enable_categorical": False
        }
        
        entrenar_con_parametros_fijos(X_train_full, X_test, y_train_full, y_test, mis_params)