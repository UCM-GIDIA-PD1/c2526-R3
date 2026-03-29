from lime import lime_tabular as lm
import numpy as np

def inicializar_explicador(X_train):
    '''
    Inicializa el explicador LIME para nuestro modelo de clasificación.
    :param X_train: conjuunto de entrenamiento.
    :return explainer: explicador.
    '''

    # Creamos el explicador LIME para regresión (lime tabular porque trabajamos con datos tabulares)
    explainer = lm.LimeTabularExplainer(
        training_data = X_train.to_numpy(), 
        feature_names=X_train.columns, 
        class_names=["No Incendio", "Incendio"],
        mode="classification",
        random_state=42
    )
    
    return explainer

def generar_explicacion(explicador, modelo, X_test):
    '''
    Genera una explicación para una instancia de test utilizando el explicador LIME.
    :param explicador: explicador LIME ya inicializado.
    :param modelo: modelo de clasificación ya entrenado.
    :param X_test: conjunto de test.
    :return exp: explicación generada por LIME.
    '''

    # Seleccionamos una instancia de test para explicar
    instancia = X_test.iloc[0].values.reshape(1, -1)

    # Generamos la explicación utilizando LIME
    exp = explicador.explain_instance(
        data_row=instancia.flatten(), 
        predict_fn=modelo.predict_proba, 
        num_features=15
    )
    
    return exp