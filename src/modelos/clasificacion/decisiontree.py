from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import fbeta_score
import wandb
from pathlib import Path

import modelos.utils.wandbFunctions as wf
import modelos.utils.carga_datos as cg
import modelos.utils.personalizacion as pers
import modelos.utils.metricas as met
import modelos.utils.particiones as part

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "decision_tree_clasificacion"
NUM_IT = 0
SWEEP_PATH = Path(__file__).with_name("decisiontree_sweep.yaml")

def decision_tree(X_train, X_val, X_test, y_train, y_val, y_test, nombre):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    if config.max_features == "None": # Daba error sino porque el YAML devuelve un string
        max_f = None
    else:
        max_f = config.max_features

    tree = DecisionTreeClassifier(
        max_depth=config.max_depth,
        min_samples_split=config.min_samples_split,
        min_samples_leaf=config.min_samples_leaf,
        criterion=config.criterion,
        max_features=max_f,
        class_weight=config.class_weight,

        random_state=42
    )

    tree.fit(X_train, y_train)

    # Evaluaciones para train
    y_prob_train = tree.predict_proba(X_train)[:, 1]
    y_pred_train = (y_prob_train >= config.umbral).astype(int)
    metricas_train = met.evaluar_clasificacion(
        y_train, y_pred_train, y_prob_train, "Entrenamiento — Decision Tree"
    )

    # Evaluaciones para validación
    y_prob_val = tree.predict_proba(X_val)[:, 1]
    y_pred_val = (y_prob_val >= config.umbral).astype(int)
    metricas_val = met.evaluar_clasificacion(
        y_val, y_pred_val, y_prob_val, "Validación — Decision Tree"
    )

    # Evaluaciones para test
    y_prob_test = tree.predict_proba(X_test)[:, 1]
    y_pred_test = (y_prob_test >= config.umbral).astype(int)
    metricas_test = met.evaluar_clasificacion(
        y_test, y_pred_test, y_prob_test, "Test — Decision Tree"
    )

    f_1_5_train = fbeta_score(y_train, y_pred_train, beta=1.5, zero_division=0)
    f_1_5_val = fbeta_score(y_val, y_pred_val, beta=1.5, zero_division=0)
    f_1_5_test = fbeta_score(y_test, y_pred_test, beta=1.5, zero_division=0)

    wandb.log({
        'f1_5_score' : f_1_5_val,

        "train/f1_5": f_1_5_train,
        "train/f1": metricas_train["f1"],
        "train/precision": metricas_train["precision"],
        "train/recall": metricas_train["recall"],
        "train/accuracy": metricas_train["accuracy"],

        "val/f1_5": f_1_5_val,
        "val/f1": metricas_val["f1"],
        "val/precision": metricas_val["precision"],
        "val/recall": metricas_val["recall"],
        "val/accuracy": metricas_val["accuracy"],

        "test/f1_5": f_1_5_test,
        "test/f1": metricas_test["f1"],
        "test/precision": metricas_test["precision"],
        "test/recall": metricas_test["recall"],
        "test/accuracy": metricas_test["accuracy"],

    })

    wf.matriz_confusion_feature_importance(tree, y_pred_val, y_val, X_train.columns.tolist())

    run.finish()

def clasificacion():

    if not wf.inicializar_apikey_wandb():
        return
    
    X, y = cg.cargar_dataset_clasificacion_todas_variables()

    X_train, X_val, X_test, y_train, y_val, y_test = part.split_estratificado(X, y)

    X_train, X_val, X_test = pers.anomalias(X_train, X_val, X_test)

    iters, nombre = pers.pregunta_iters_nombre()

    def entrenamiento():
        decision_tree(X_train, X_val, X_test, y_train, y_val, y_test, nombre)

    sweep_id = wf.crear_sweep_id(WANDB_PROJECT, SWEEP_PATH)

    wandb.agent(
        sweep_id=sweep_id,
        function=entrenamiento,
        count=iters,
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
    )

    print(f"Listo!! Ejecutadas {iters} iteraciones")

if __name__ == "__main__":
    clasificacion()
