from typing import Literal
from modelos.clasificacion import balanced_random_forest, decisiontree, m_xgboost, random_forest, regresion_logistica
from modelos.generico import modelo_xgboost
from modelos.regresion.arboles import frp_xgBoost
from modelos.regresion.arboles import frp_rdForest


def evaluacion_modelo(modelo:Literal["XGBoostClassifier", "BalancedRandomForest", "DecisionTree", "RandomForestClassifier", "Regresión logística", "RandomForestFRP", "XGBoostFRP"]):
    '''
    Función para evaluar los modelos finales en nuestro conjunto de validación
    '''
    metodo = input("Indica el método empleado (grid/random/bayes): ").lower()
    hiperparametros = pedir_hiperparametros(modelo)
    
    if modelo == "XGBoostClassifier":
        m_xgboost.evaluacion_final(hiperparametros, metodo)

    elif modelo == "BalancedRandomForest":
        balanced_random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "DecisionTree":
        decisiontree.evaluacion_final(hiperparametros, metodo)

    elif modelo == "RandomForestClassifier":
        random_forest.evaluacion_final(hiperparametros, metodo)

    elif modelo == "Regresión logística":
        regresion_logistica.evaluacion_final(hiperparametros, metodo)

    elif modelo == 'RandomForestFRP':
        frp_rdForest.evaluacion_final(hiperparametros, metodo)

    elif modelo == 'XGBoostFRP':
        frp_xgBoost.evaluacion_final(hiperparametros, metodo)


def pedir_hiperparametros(modelo):
    '''
    Función que solicita los hiperparámetros y los convierte al tipo correcto
    '''
    hiperparametros = {}
    print(f"\n--- Introduciendo hiperparámetros para {modelo} ---")

    if modelo != "Regresión logística":
        hiperparametros["max_depth"] = int(input("max_depth: "))
        hiperparametros["n_estimators"] = int(input("n_estimators: "))
    
    if modelo != "Regresión logística" and 'FRP' not in modelo:
        hiperparametros["umbral"] = float(input("umbral (float, ej 0.35): "))

    if any(model in modelo for model in ["RandomForest", "DecisionTree", "BalancedRandomForest"]):
        hiperparametros["min_samples_split"] = int(input("min_samples_split: "))
        hiperparametros["min_samples_leaf"] = int(input("min_samples_leaf: "))
        hiperparametros["criterion"] = input("criterion (gini/entropy): ")
        
        mf = input("max_features (sqrt/log2/None): ")
        hiperparametros["max_features"] = None if mf.lower() == "none" else mf

    if modelo == 'XGBoostFRP':
        incluir_tweedie = input('Quieres incluir la distribucion tweedie? (s/n): ').lower()
        if incluir_tweedie == 's':
            hiperparametros["objective"] = "reg:tweedie"
            hiperparametros["tweedie_variance_power"] = float(input("tweedie_variance_power (float): "))
  
    if modelo == "XGBoostClassifier" or modelo == "XGBoostFRP":
        hiperparametros["learning_rate"] = float(input("learning_rate (float): "))
        hiperparametros["subsample"] = float(input("subsample (0.5-1): "))
        hiperparametros["colsample_bytree"] = float(input("colsample_bytree (0.5-1): "))

    if modelo == "XGBoostFRP":
        hiperparametros["min_child_weight"] = float(input("min_child_weight: "))
        hiperparametros["gamma"] = float(input("gamma (float): "))
        hiperparametros["reg_alpha"] = float(input("reg_alpha (float): "))
        hiperparametros["reg_lambda"] = float(input("reg_lambda (float): "))

    
    if modelo == "Regresión logística":
        hiperparametros["penalty"] = input("penalty (l1/l2/None): ")
       
        if str(hiperparametros["penalty"]).lower() == "none":
            hiperparametros["penalty"] = None
        hiperparametros["umbral"] = float(input("umbral (float): "))

    if modelo in ["RandomForestClassifier", "DecisionTree", "Regresión logística"]:
        cw = input("class_weight (balanced/None): ")
        hiperparametros["class_weight"] = None if cw.lower() == "none" else cw

    return hiperparametros

