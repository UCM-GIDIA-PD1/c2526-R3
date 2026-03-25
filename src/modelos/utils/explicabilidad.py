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

'''
if __name__ == "__main__":
    cliente = mf.crear_cliente()
    df = mf.bajar_fichero(cliente, "grupo3/cleaned/final_date_transformado.parquet")
    
    X = df.drop(["final"], axis=1)
    y = df["final"]
    class_names = ["No Incendio", "Incendio"]
    feature_names = X.columns

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    params_xgb = {
            'random_state': 42,
            'use_label_encoder': False,
            'eval_metric': 'logloss'
    }
    clf = xgb.XGBClassifier(**params_xgb)
    clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    explicador = inicializar_explicador(X_train)
    exp = generar_explicacion(explicador, clf, X_test)
    fig_lime = exp.as_pyplot_figure()
    plt.show()
'''
