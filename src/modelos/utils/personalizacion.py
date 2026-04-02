import limpieza.limpieza as clean
import modelos.utils.carga_datos as cg
import modelos.utils.anomalias as anom
import extraccion.minioFunctions as mf
import pandas as pd

def pregunta_PCA(clasificacion = True, df=None):
    '''
    Pide por pantalla si se quiere aplicar PCA. 
    Si recibe un df, lo usa. Si no, lo descarga.
    '''

    if df is None:
        df = clean.bajar_df_final(clasificacion) 

    df.columns = df.columns.str.lower().str.strip()
    if clasificacion:
        target_col = 'incendio' if 'incendio' in df.columns else 'final'
    else:
        target_col = 'frp_mean'

    while True:
        pca_input = input("¿Quieres aplicar PCA a los datos? (s/n): ").lower()
        
        if pca_input == 's':
            from sklearn.decomposition import PCA
            n_components = int(input('¿Cuántos componentes quieres usar? '))
            df = df.sort_values(by='date')
            X_raw = df.drop(columns=[target_col, 'date'], errors='ignore')
            y = df[target_col]
            
            pca_model = PCA(n_components=n_components)
            X_pca = pca_model.fit_transform(X_raw)
            X = pd.DataFrame(X_pca, columns=[f'PC{i+1}' for i in range(n_components)])
            break
            
        elif pca_input == 'n':
            X = df.drop(columns=[target_col], errors='ignore')
            y = df[target_col]
            break
        else:
            print("Entrada no válida. Por favor, ingresa 's' o 'n'.")
    
    return X, y

def pregunta_iters_nombre():
    iters = int(input("¿Cuantas iteraciones quieres ejecutar? : "))
    nombre = input("Como quieres llamar a los runs de este sweep?"
                "(se aplicará un indice a cada run para que no se sobreescriban) ")
    
    return iters, nombre

def anomalias(X_train_full, X_test):
    '''
    Pregunta al usuario qué análisis de anomalías quiere aplicar a los datos 
    y se lo aplica, devolviendo los datasets actualizados (Train y Test).
    '''

    print("\n--- Selección de Análisis de Anomalías ---")
    print("1. Isolation Forest")
    print("2. One Class SVM")
    print("3. Local Outlier Factor (LOF)")
    print("Pulsa Enter sin escribir nada para no aplicar ningún análisis de anomalías.")
    print("(Puedes seleccionar varios separados por comas, ej: 1,3)")

    entrada = input("Elige las opciones: ")

    if not entrada:
        print("No se aplicará ningún análisis de anomalías.")
        return X_train_full, X_test

    opciones = [opt.strip() for opt in entrada.split(",")]

    if '1' in opciones:
        print("Aplicando Isolation Forest...")
        X_train_full, X_test = anom.isolationForest(X_train_full, X_test)
        print("Aplicado Isolation Forest!")
        
    if '2' in opciones or '3' in opciones:
         print("Realizando escalado y PCA...")
         X_train_PCA, X_test_PCA = anom.escalado_PCA(X_train_full, X_test)
         print("Escalado y PCA realizados!")
         
    if '2' in opciones:
        print("Aplicando One Class SVM...")
        X_train_full, X_test = anom.oneClassSVM(X_train_PCA, X_test_PCA, X_train_full, X_test)
        print("Aplicado One Class SVM!")
        
    if '3' in opciones:
        print("Aplicando Local Outlier Factor (LOF)...")
        X_train_full, X_test = anom.LOF(X_train_PCA, X_test_PCA, X_train_full, X_test)
        print("Aplicado Local Outlier Factor (LOF)!")

    return X_train_full, X_test