import wandb
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from extraccion import minioFunctions as mf
import pandas as pd
import os
import numpy as np

mf.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')

def wandb_init():
    run = wandb.init(
        # Set the wandb entity where your project will be logged (generally your team name).
        entity="pd1-c2526-team3",
        # Set the wandb project where this run will be logged.
        project="arboles-decision-generales",
        
        # Track hyperparameters and run metadata.
        config={
            "max_depth": 3,
            "criterion": "gini",
            "test_size": 0.2
        },
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

def arboles_decision_sin_filtrado():
    wandb_init()
    config = wandb.config

    data = coger_dfs()

    target = np.array(data['final'])
    x = np.array(data.drop(columns=['final', 'date']))

    X_train, X_test, y_train, y_test = train_test_split(
    x, target, test_size=config.test_size
    )

    model = DecisionTreeClassifier(max_depth=config.max_depth, criterion=config.criterion)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_probas = model.predict_proba(X_test)

    nombres_clases = [str(clase) for clase in model.classes_]

    wandb.sklearn.plot_classifier(
        model, X_train, X_test, y_train, y_test, y_pred, y_probas, 
        labels=nombres_clases, model_name='DecisionTree'
    )

    wandb.finish()

if __name__ == '__main__':
    # print(coger_dfs())
    arboles_decision_sin_filtrado()
    