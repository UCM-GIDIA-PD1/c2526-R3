from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import wandb
import xgboost as xgb
from sklearn.metrics import f1_score, recall_score, fbeta_score

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
    '''
    Menú para elegir modelo XGBoost a ejecutar
    '''
    print("Opciones: ")
    print("1. XGBoost con aplicación de ventanas temporales")
    print("2. XGBoost con aplicación de ventanas temporales y anomalías")
    opcion = int(input("Elige opcion [1,2]: "))
    assert opcion in [1, 2], "Número no válido, elige entre 1 y 2."

    print("¿Qué dataframe quieres usar? (Si dejas vacío se usará el MINI por defecto)")
    df = input("Nombre del dataframe en MinIO (p.e. 'MINI'): ")
    if not df:
        df = "MINI"

    return opcion, df

def funcionalidad_tags():
    args = parser.initialite_parser()
    tags = []
    if args.modelo: 
        tags.append(args.modelo)
    tags = args.tags + [f"correladas_{args.eliminar_correladas}"]
    return tags

def configuraciones_iniciales(df):
    '''
    Configuración inicial para el modelo XGBoost
    :param df: Nombre del dataframe en MinIO (p.e. "MINI"))
    :return tags: Lista de tags para wandb
    :return df: DataFrame con los datos'''

    tags = funcionalidad_tags()
    
    #Conexion con MinIO (bajamos dataframe)
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, f"grupo3/cleaned/{df}.parquet", "df")
    assert df is not None, f"Fallo en la descarga de {df} desde MinIO (conecta la VPN o revisa el nombre del dataframe)"

    #Manipulación de columnas
    if 'final' in df.columns:
        df = df.rename(columns={'final': 'incendio'})
    assert 'incendio' in df.columns, "La columna 'incendio' no se encuentra en el DataFrame."
    print("Columnas actuales en el DF:", df.columns.tolist())

    return tags, df

def ventanas_temporales(df):
    '''
    Modelo XGBoost con aplicación de ventanas temporales

    :param df: Nombre del dataframe en MinIO (p.e. "MINI"))
    :return tags: Lista de tags para wandb
    :return feature_names: Lista de nombres de las variables
    :return X_train, X_test, y_train, y_test: particion en test y train

    '''
    #Bajamos df, aplicamos ventanas temporales y PCA (si se desea)
    tags, df_normal = configuraciones_iniciales(df)
    df_transformado = vt.menu_ventanas_temporales(df_normal)
    X, y = per.pregunta_PCA(df_transformado) 

    #División de los datos
    X_train, X_test, y_train, y_test = split_temporal(X, y)
    feature_names = [f"Var_{i}" for i in range(X_train.shape[1])]

    return tags, feature_names, X_train, X_test, y_train, y_test

def ventanas_temporales_y_anomalias(df):
    '''
    Modelo XGBoost con aplicación de ventanas temporales y análisis de anomalías

    :param df: Nombre del dataframe en MinIO (p.e. "MINI"))
    :return tags: Lista de tags para wandb
    :return feature_names: Lista de nombres de las variables
    :return X_train, X_test, y_train, y_test: particion en test y train
    '''
    #Aplicamos ventanas temporales 
    tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales(df)

    #Aplicamos análisis de anomalías
    X_train, X_test = per.anomalias(X_train, X_test)
    feature_names = [f"Var_{i}" for i in range(X_train.shape[1])]

    return tags, feature_names, X_train, X_test, y_train, y_test

def explicabilidad_lime(clasificador, X_train, X_test):
    '''
    Genera explicaciones LIME para el modelo entrenado y las sube a wandb.

    :param clasificador: Modelo entrenado
    :param X_train, X_test: conjunto test y train de las variables explicativas
    '''
    #Parseamos los datos
    X_train_lime = X_train.fillna(0)
    X_test_lime = X_test.fillna(0)
    
    #Generamos explicaciones LIME
    explicador = exp.inicializar_explicador(X_train_lime)
    explicacion_lime = exp.generar_explicacion(explicador, clasificador, X_test_lime)
    
    #Guardamos la explicación LIME como imagen y la subimos a wandb
    fig_lime = explicacion_lime.as_pyplot_figure()
    plt.tight_layout()
    wandb.log({"explicabilidad/lime": wandb.Image(fig_lime)})
    plt.close(fig_lime)

def train(tags, class_names, feature_names, X_train_full, X_test, y_train_full, y_test):
    '''
    Función principal de entrenamiento del modelo XGBoost
    con validación cruzada estratificada y subida de métricas a wandb.
    '''
    X_train_full = X_train_full.fillna(0)
    X_test = X_test.fillna(0)

    # Entrenamos el modelo con validación cruzada estratificada y subimos métricas a wandb
    with wandb.init(settings=wandb.Settings(start_method="thread"), tags=tags) as run:
        config = wandb.config
        umbral = getattr(config, 'umbral_decision', 0.5)
        
        # Creamos el modelo XGBoost con los hiperparámetros del sweep
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

        # Validación cruzada estratificada
        cv_generator = generador_cv(tipo_cv="estratificado", n_splits=4, seed=SEED)
        cv_f1, cv_f2, cv_recall = [], [], []
    
        # Aplicamos la validación cruzada estratificada 
        for t_idx, v_idx in cv_generator.split(X_train_full, y_train_full):
            x_full_train, x_full_validate = X_train_full.iloc[t_idx], X_train_full.iloc[v_idx]
            y_full_train, y_full_validate = y_train_full.iloc[t_idx], y_train_full.iloc[v_idx]
            clf.fit(x_full_train, y_full_train)
            y_f_prob = clf.predict_proba(x_full_validate)[:, 1]
            y_f_pred = (y_f_prob >= umbral).astype(int)
            cv_f1.append(f1_score(y_full_validate, y_f_pred, zero_division=0))
            cv_f2.append(fbeta_score(y_full_validate, y_f_pred, beta=2, zero_division=0))
            cv_recall.append(recall_score(y_full_validate, y_f_pred, zero_division=0))

        # Entrenamos el modelo con todo el conjunto de entrenamiento
        clf.fit(X_train_full, y_train_full)
        y_probas = clf.predict_proba(X_test)
        y_pred = (y_probas[:, 1] >= umbral).astype(int)

        # Evaluamos el modelo del conjunto de test y subimos métricas a wandb
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

        # Subimos gráficas a wandb
        wf.matriz_confusion_feature_importance(clf, y_pred, y_test.to_numpy(), feature_names)
        
        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close()

        # Aplicamos explicabilidad LIME para este modelo de caja negra  
        explicabilidad_lime(clf, X_train_full, X_test)

if __name__ == "__main__":
    assert wf.inicializar_apikey_wandb()
    wandb.login() 

    opcion, df = menu()
    if opcion == 1:
        tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales(df)
    elif opcion == 2:
        tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales_y_anomalias(df)

    class_names = ["No Incendio", "Incendio"]
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
        wandb.agent(sweep_id, 
                    function=lambda: train(tags, class_names, feature_names, 
                                           X_train, X_test, y_train, y_test), count=15)