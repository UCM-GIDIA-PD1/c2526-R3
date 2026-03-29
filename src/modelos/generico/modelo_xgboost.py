import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import wandb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, recall_score, precision_score, fbeta_score

from extraccion import minioFunctions 
from modelos import parser
from modelos.utils import personalizacion as per, wandbFunctions as wf, explicabilidad as exp
from modelos.utils.particiones import split_temporal, generador_cv
from modelos.utils.metricas import evaluar_clasificacion
from modelos.clasificacion import ventanas_temporales as vt

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SEED = 42

def menu():
    print("Opciones: ")
    print("1. XGBoost con configuraciones normales")
    print("2. XGBoost con aplicación de ventanas temporales y anomalías")
    opcion = int(input("Elige opcion [1,2]: "))
    assert opcion in [1, 2], "Número no válido"
    return opcion

def funcionalidad_tags():
    args = parser.initialite_parser()
    tags = []
    if args.modelo: 
        tags.append(args.modelo)
    tags = args.tags + [f"correladas_{args.eliminar_correladas}"]
    return tags

def configuraciones_iniciales():
    tags = funcionalidad_tags()
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, "grupo3/cleaned/MINI.parquet", "df")

    if 'final' in df.columns:
        df = df.rename(columns={'final': 'incendio'})

    X = df.drop(["incendio"], axis=1)
    y = df["incendio"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=SEED)
    
    return tags, ["No Incendio", "Incendio"], X.columns, X_train, X_test, y_train, y_test

def ventanas_temporales_y_anomalias():
    tags = funcionalidad_tags()
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, "grupo3/cleaned/MINI.parquet", "df")
    
    if 'final' in df.columns:
        df = df.rename(columns={'final': 'incendio'})

    print("Columnas actuales en el DF:", df.columns.tolist())
    df = vt.menu_ventanas_temporales(df)
    X, y = per.pregunta_PCA(df) 

    resultado_split = split_temporal(X, y)
    X_train, X_test, y_train, y_test = resultado_split[0], resultado_split[1], resultado_split[2], resultado_split[3]
    
    X_train, X_test = per.anomalias(X_train, X_test)

    feature_names = [f"Var_{i}" for i in range(X_train.shape[1])]
    return tags, ["No Incendio", "Incendio"], feature_names, X_train, X_test, y_train, y_test

def explicabilidad_lime(clasificador, X_train, X_test):
    X_train_lime = X_train.fillna(0)
    X_test_lime = X_test.fillna(0)
    
    explicador = exp.inicializar_explicador(X_train_lime)
    explicacion_lime = exp.generar_explicacion(explicador, clasificador, X_test_lime)
    
    fig_lime = explicacion_lime.as_pyplot_figure()
    plt.tight_layout()
    wandb.log({"explicabilidad/lime": wandb.Image(fig_lime)})
    plt.close(fig_lime)

def train(tags, class_names, feature_names, X_train_full, X_test, y_train_full, y_test):
    X_train_full = X_train_full.fillna(0)
    X_test = X_test.fillna(0)

    with wandb.init(settings=wandb.Settings(start_method="thread"), tags=tags) as run:
        config = wandb.config
        umbral = getattr(config, 'umbral_decision', 0.5)
        
        clf = xgb.XGBClassifier(
            n_estimators=getattr(config, 'n_estimators', 100),
            learning_rate=getattr(config, 'learning_rate', 0.1),
            max_depth=getattr(config, 'max_depth', 6),
            scale_pos_weight=getattr(config, 'scale_pos_weight', 1),
            subsample=getattr(config, 'subsample', 1.0),
            colsample_bytree=getattr(config, 'colsample_bytree', 1.0),
            random_state=SEED,
            eval_metric='logloss'
        )

        cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
        cv_f1, cv_f2, cv_recall = [], [], []

        for t_idx, v_idx in cv_generator.split(X_train_full, y_train_full):
            x_full_train, x_full_validate = X_train_full.iloc[t_idx], X_train_full.iloc[v_idx]
            y_full_train, y_full_validate = y_train_full.iloc[t_idx], y_train_full.iloc[v_idx]
            clf.fit(x_full_train, y_full_train)
            y_f_prob = clf.predict_proba(x_full_validate)[:, 1]
            y_f_pred = (y_f_prob >= umbral).astype(int)
            cv_f1.append(f1_score(y_full_validate, y_f_pred, zero_division=0))
            cv_f2.append(fbeta_score(y_full_validate, y_f_pred, beta=2, zero_division=0))
            cv_recall.append(recall_score(y_full_validate, y_f_pred, zero_division=0))

        clf.fit(X_train_full, y_train_full)
        y_probas = clf.predict_proba(X_test)
        y_pred = (y_probas[:, 1] >= umbral).astype(int)

        metricas = evaluar_clasificacion(y_test, y_pred, y_probas[:, 1], "Test")
        wandb.log({
            "val/f1_mean_cv": np.mean(cv_f1),
            "val/f2_mean_cv": np.mean(cv_f2),
            "val/recall_mean_cv": np.mean(cv_recall),
            "test/f1": metricas["f1"],
            "test/recall": metricas["recall"],
            "test/precision": metricas["precision"],
            "test/accuracy": metricas["accuracy"],
            "graficas/roc": wandb.plot.roc_curve(y_test, y_probas, labels=class_names),
            "graficas/pr": wandb.plot.pr_curve(y_test, y_probas, labels=class_names)
        })

        wf.matriz_confusion_feature_importance(clf, y_pred, y_test.to_numpy(), feature_names)
        
        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close()

        explicabilidad_lime(clf, X_train_full, X_test)

if __name__ == "__main__":
    assert wf.inicializar_apikey_wandb()
    wandb.login() 

    opcion = menu()
    if opcion == 1:
        tags, class_names, feat_names, X_tr, X_te, y_tr, y_te = configuraciones_iniciales()
    else:
        tags, class_names, feat_names, X_tr, X_te, y_tr, y_te = ventanas_temporales_y_anomalias()

    configuraciones = {
        "sweep_xgboost_incendios": {
            'method': 'bayes',
            'metric': {'name': 'val/f1_mean_cv', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'distribution': 'uniform', 'min': 0.01, 'max': 0.2},
                'max_depth': {'values': [3, 6, 9]},
                'n_estimators': {'values': [100, 300, 500]},
                'scale_pos_weight': {'values': [1, 5, 10]},
                'umbral_decision': {'distribution': 'uniform', 'min': 0.25, 'max': 0.6}
            }
        }
    }

    for nombre_config, config in configuraciones.items():
        sweep_id = wandb.sweep(config, project=WANDB_PROJECT, entity=WANDB_ENTITY)
        wandb.agent(sweep_id, function=lambda: train(tags, class_names, feat_names, X_tr, X_te, y_tr, y_te), count=15)