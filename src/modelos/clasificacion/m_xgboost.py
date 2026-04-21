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
from sklearn.metrics import confusion_matrix
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers
import modelos.clasificacion.ventanas_temporales as ventana
import modelos.utils.explicabilidad as exp

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SEED = 42
NUM_IT = 0

def explicabilidad_lime(clasificador, X_train, X_test):
    '''
    Función para generar la explicación LIME de un clasificador dado y subirla a wandb.
    
    :param clasificador: clasificador entrenado (xgboost)
    :param X_train, X_test: conjunto de variables explicativas de train y test
    '''
    # Saneamos los valores nulos porque Lime no los acepta
    X_train_lime = X_train.fillna(0)
    X_test_lime = X_test.fillna(0)

    # Inicializamos el explicador LIME (con X_train)y generamos la explicación (con X_test)
    explicador = exp.inicializar_explicador(X_train_lime)
    explicacion_lime = exp.generar_explicacion(explicador, clasificador, X_test_lime)

    # Ajustes para que el gráfico de LIME se vea bien en wandb
    fig_lime = explicacion_lime.as_pyplot_figure()
    plt.tight_layout()
    wandb.log({"explicabilidad/lime": wandb.Image(fig_lime)})
    plt.close(fig_lime)

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
    explicabilidad_lime(clf, X_train_full, X_test)

    run.finish()


def entrenamiento(X_train_full, y_train_full, nombre = None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    # Hago esto para ver si le puedo dar más peso a la clase minoritaria (Incendios)

    ratio = calcular_ratio_clases(y_train_full)

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    f2_cv_scores, f1_cv_scores, recall_cv_scores = [], [], []
    f2_cv_scores_train, f1_cv_scores_train= [], []
    best_iterations = []
    tns, fps, fns, tps = [], [], [], []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        clf = xgb.XGBClassifier(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            subsample=config.subsample,
            colsample_bytree=config.colsample_bytree,
            min_child_weight=config.get("min_child_weight", 1),
            gamma=config.get("gamma", 0),
            scale_pos_weight=ratio,
            random_state=SEED,
            eval_metric="aucpr",
            early_stopping_rounds=100, 
            n_jobs=-1,
        )
        
        # Modifico el clf.fit para que el entrenamiento se detenga si no mejora en 50 árboels (a ver si se evita así el overfitting)

        clf.fit(
            X_fold_train, y_fold_train,
            eval_set=[(X_fold_val, y_fold_val)],
            verbose=False
        )

        # Añado la mejor iteración automáticamente
        best_iterations.append(clf.best_iteration)

        # Métricas de validation
        y_val_prob = clf.predict_proba(X_fold_val)[:, 1]
        y_fold_pred = (y_val_prob >= config.umbral).astype(int)
        f1_cv_scores.append(f1_score(y_fold_val, y_fold_pred, zero_division=0))
        f2_cv_scores.append(fbeta_score(y_fold_val, y_fold_pred, beta=2, zero_division=0))
        recall_cv_scores.append(recall_score(y_fold_val, y_fold_pred, zero_division=0))

        y_train_prob = clf.predict_proba(X_fold_train)[:, 1]
        y_fold_pred_train = (y_train_prob >= config.umbral).astype(int)
        f1_cv_scores_train.append(f1_score(y_fold_train, y_fold_pred_train, zero_division=0))
        f2_cv_scores_train.append(fbeta_score(y_fold_train, y_fold_pred_train, beta=2, zero_division=0))
        
        cm = confusion_matrix(y_fold_val, y_fold_pred)

        tns.append(cm[0,0])
        fps.append(cm[0,1])
        fns.append(cm[1,0])
        tps.append(cm[1,1])

    wandb.log({
        "train/f1_mean_cv": float(np.mean(f1_cv_scores_train)),
        "train/f2_mean_cv": float(np.mean(f2_cv_scores_train)),
        "val/f1_mean_cv": float(np.mean(f1_cv_scores)),
        "val/f2_mean_cv": float(np.mean(f2_cv_scores)),
        "val/f1_std_cv": float(np.std(f1_cv_scores)), 
        "val/recall_mean_cv": float(np.mean(recall_cv_scores)),
        "diff/f1_overfit": float(np.mean(f1_cv_scores_train) - np.mean(f1_cv_scores)),
        "best_iteration_mean": float(np.mean(best_iterations)),
        "scale_pos_weight": ratio,
        "val/tn_mean": np.mean(tns),
        "val/fp_mean": np.mean(fps),
        "val/fn_mean": np.mean(fns),
        "val/tp_mean": np.mean(tps)
    })

    run.finish()

    # Me he cargado los gráficos pq no tenía mucho sentido mirar los gráficos del entrenamiento, en entrenamiento solo queremos buscar los hiperparámetros



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
    
    if "id_hexagono" in df_features.columns:
        df_features = df_features.drop(columns=['id_hexagono'])
        
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