import limpieza.limpieza as clean
import modelos.utils.carga_datos as cg
import modelos.utils.anomalias as anom
import extraccion.minioFunctions as mf

def pregunta_PCA():
    '''
    Pide por pantalla si se quiere aplicar PCA a los datos o no y devuelve
    X e y en función de la decisión del usuario.
    '''
    while True:
         pca = input("¿Quieres aplicar PCA a los datos? (s/n) : ")
         if pca.lower() == 's':
            comps = int(input('Cuantos componentes quieres usar? '))
            df = clean.bajar_df_final()
            X = df.drop(columns = ['fires'])
            y = df['fires']
            break
         elif pca.lower() == 'n':
             X,y = cg.cargar_dataset_general(eliminar_correladas=False)
             break
         else:
             print("Entrada no válida. Por favor, ingresa 's' para sí o 'n' para no.")
    
    return X,y

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