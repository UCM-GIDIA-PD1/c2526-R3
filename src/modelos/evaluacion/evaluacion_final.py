from modelos.clasificacion import balanced_random_forest, balanced_rf_f1, decisiontree, m_xgboost, random_forest
# , regresion_logistica
from modelos.generico import modelo_xgboost
# from modelos.regresion import frp_rdForest, frp_xgBoost
from modelos.utils.particiones import split_temporal
from modelos.utils.carga_datos import cargar_dataset_general, cargar_dataset_frp


def evaluacion_modelo(modelo, tipo_modelo):
    '''
    Función para evaluar los modelos finales en nuestro conjunto de validación
    '''
    metodo = input("Indica el método que has empleado en la búsqueda de hiperparámetros: ")

    hiperparametros = pedir_hiperparametros(modelo, tipo_modelo)
    
    if modelo == "XGBoost":
        m_xgboost.evaluacion_final(hiperparametros)

    elif modelo == "BalancedRandomForest" and tipo_modelo == "clasificación":
        balanced_random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "DecisionTree" and tipo_modelo == "clasificación":
        decisiontree.evaluacion_final(hiperparametros, metodo)

    elif modelo == "RandomForest" and tipo_modelo == "clasificación":
        random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "BalancedRandomForest" and tipo_modelo == "clasificación":
        balanced_random_forest.evaluacion_final(hiperparametros)

    elif modelo == "Regresión logística":
        print("Hiperparámetros:", '\n')
        # regresion_logistica.evaluacion_final(hiperparametros)


def pedir_hiperparametros(modelo, tipo_modelo):
    '''
    Función que solicita los hiperparámetros óptimos
    '''
    
    hiperparametros = {}
    print("Hiperparámetros:", '\n')

    if modelo != "Regresión logística":
        hiperparametros["max_depth"] = input("max_depth: ")
        hiperparametros["n_estimators"] = input("n_estimators: ")
        hiperparametros["umbral"] = input("umbral: ")

    if modelo == "RandomForest" or modelo == "DecisionTree" or modelo == "BalancedRandomForest":
        hiperparametros["min_samples_split"] = input("min_samples_split: ")
        hiperparametros["min_samples_leaf"] = input("min_samples_leaf: ")
        hiperparametros["criterion"] = input("criterion: ")
        hiperparametros["max_features"] = input("max_features: ")

    if modelo == "XGBoost":
        hiperparametros["learning_rate"] = input("learning_rate: ")
        hiperparametros["subsample"] = input("subsample: ")
        hiperparametros["colsample_bytree"] = input("colsample_bytree: ")

    elif modelo == "RandomForest" or modelo == "DecisionTree":
        hiperparametros["class_weight"] = input("class_weight: ")


    return hiperparametros

