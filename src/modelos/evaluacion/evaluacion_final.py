from modelos.clasificacion import balanced_random_forest, balanced_rf_f1, decisiontree, m_xgboost, random_forest, regresion_logistica
from modelos.generico import modelo_xgboost
from modelos.regresion import frp_rdForest, frp_xgBoost
from modelos.utils.particiones import split_temporal
from modelos.utils.carga_datos import cargar_dataset_general, cargar_dataset_frp


def evaluacion_modelo(modelo, tipo_modelo):
    '''
    Función para evaluar los modelos finales en nuestro conjunto de validación
    '''
    metodo = input("Indica el método que has empleado en la búsqueda de hiperparámetros: ")
    
    if modelo == "XGBoost":
        print("Hiperparámetros:", '\n')
        # m_xgboost.evaluacion_final(hiperparametros)

    elif modelo == "RandomForest" and tipo_modelo == "clasificación":
        print("Hiperparámetros:", '\n')
        hiperparametros["max_depth"] = input("max_depth: ")
        hiperparametros["n_estimators"] = input("n_estimators: ")
        hiperparametros["min_samples_split"] = input("min_samples_split: ")
        hiperparametros["min_samples_leaf"] = input("min_samples_leaf: ")
        hiperparametros["criterion"] = input("criterion: ")
        hiperparametros["class_weight"] = input("class_weight: ")
        hiperparametros["max_features"] = input("max_features: ")
        hiperparametros["umbral"] = input("umbral: ")

        random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "DecisionTree" and tipo_modelo == "clasificación":
        print("Hiperparámetros:", '\n')
        hiperparametros["max_depth"] = input("max_depth: ")
        hiperparametros["n_estimators"] = input("n_estimators: ")
        hiperparametros["min_samples_split"] = input("min_samples_split: ")
        hiperparametros["min_samples_leaf"] = input("min_samples_leaf: ")
        hiperparametros["criterion"] = input("criterion: ")
        hiperparametros["class_weight"] = input("class_weight: ")
        hiperparametros["max_features"] = input("max_features: ")
        hiperparametros["umbral"] = input("umbral: ")

        random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "BalancedRandomForest" and tipo_modelo == "clasificación":
        hiperparametros = {}

        print("Hiperparámetros:", '\n')
        hiperparametros["max_depth"] = input("max_depth: ")
        hiperparametros["n_estimators"] = input("n_estimators: ")
        hiperparametros["min_samples_split"] = input("min_samples_split: ")
        hiperparametros["min_samples_leaf"] = input("min_samples_leaf: ")

        balanced_random_forest.evaluacion_final(hiperparametros)

    elif modelo == "Regresión logística":
        print("Hiperparámetros:", '\n')
        # regresion_logistica.evaluacion_final(hiperparametros)
