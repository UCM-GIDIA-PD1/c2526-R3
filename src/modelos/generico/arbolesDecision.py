from sklearn.metrics import fbeta_score
import wandb
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from extraccion import minioFunctions as mf
import pandas as pd
import os
import numpy as np
import yaml

mf.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')


def cargar_configuracion(): # No se usa porque se hace automatico

    # Carga la configuración del sweep desde el archivo YAML
    with open('randomforest_sweep.yaml', 'r') as f:
        config = yaml.safe_load(f)

    return config

def wandb_init():
    #nombre = input("Introduce el nombre del experimento: ")
    
    run = wandb.init(
        # Set the wandb entity where your project will be logged (generally your team name).
        entity="pd1-c2526-team3",
        # Set the wandb project where this run will be logged.
        project="arboles-decision-sweeps",

        #project="arboles-decision-generales",
        # Track hyperparameters and run metadata.
    )

def coger_dfs():
    anios = [2022, 2023, 2024, 2025]
    dfs =[]
    for k in anios:
        print(f'Seleccionando el año {k}')
        df = mf.bajar_fichero(mf.crear_cliente(),  path_server=f'grupo3/cleaned/modelo_General_{k}.parquet', type='df')
        dfs.append(df)
    data = pd.concat(dfs, ignore_index=True)
    return data

def datos():
    data = coger_dfs()

    target = data['final']
    x = data.drop(columns=['final', 'date', 'lat', 'lon'])

    X_train, X_test, y_train, y_test = train_test_split(
    x, target, test_size= 0.2, random_state=42
    )

    return X_train, X_test, y_train, y_test

def arboles_decision_sin_filtrado(X_train, X_test, y_train, y_test):
    wandb_init()
    config = wandb.config

    model = RandomForestClassifier(max_depth=config.max_depth, criterion=config.criterion,
                                    n_estimators=config.n_estimators,class_weight=config.class_weight,
                                    min_samples_leaf=config.min_samples_leaf, min_samples_split=config.min_samples_split,
                                      random_state=42)
    
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_probas = model.predict_proba(X_test)

    nombres_clases = [str(clase) for clase in model.classes_]

    # Calcula el F-score con beta=1.5, penalizando más los falsos negativos
    f1_5 = fbeta_score(y_test, y_pred, beta=1.5)
    wandb.log({"f1_5_score": f1_5})

    wandb.sklearn.plot_classifier(
        model, X_train, X_test, y_train, y_test, y_pred, y_probas, 
        labels=nombres_clases, model_name='RandomForest', feature_names=X_train.columns.tolist()
    )

    wandb.finish()



if __name__ == '__main__':

    # Para solo tener que sacar los datos de minio una vez
    X_train, X_test, y_train, y_test = datos()

    def entrenamiento():
        arboles_decision_sin_filtrado(X_train, X_test, y_train, y_test)

    #Inicia el agente , count es el numero de ejecuciones
    wandb.agent(sweep_id="l241peqb", function= entrenamiento, count=25, entity = "pd1-c2526-team3", project="arboles-decision-sweeps")

    # print(coger_dfs().iloc[:, [8, 15, 16, 1, 6]])
    #arboles_decision_sin_filtrado(X_train, X_test, y_train, y_test)
    