import pandas as pd
from extraccion import minioFunctions as mf
from limpieza import bajar_df_final
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler 

def pca():
    '''
    Aplicamos PCA a nuestro dataset final para reducir la dimensionalidad

    :return df_pca: dataframe con las componentes principales y la variable objetivo
    :return df_metricas: dataframe con las métricas (de momento solo he incluído la varianza).
    '''
    df = bajar_df_final()
    
    # Separamos las características y variable objetivo
    X = df.drop(columns=["final", "date"])
    y = df["final"]
    
    #Estandarizamos los datos
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Aplicamos PCA
    pca = PCA(n_components=None) 
    X_pca = pca.fit_transform(X_scaled)
    
    # Creamos un nuevo dataframe con las componentes principales
    componentes = [f"PC{i+1}" for i in range(X_pca.shape[1])]
    df_pca = pd.DataFrame(X_pca, columns=componentes)
    df_pca["final"] = y.values
    
    # Subimos el nuevo dataframe a minio
    cliente = mf.crear_cliente()
    mf.subir_fichero(cliente, "grupo3/cleaned/pca/final_pca.parquet", df_pca)

    #Obtenemos las métricas del pca para su posterior análisis y las subimos a minio como parquet
    varianza = pca.explained_variance_ratio_
    
    df_metricas = pd.DataFrame({
        'Componente': [f'PC{i+1}' for i in range(len(varianza))],
        'Varianza_Explicada': varianza,
    })

    mf.subir_fichero(cliente, "grupo3/cleaned/pca/metricas_pca.parquet", df_metricas)

    return df_pca, df_metricas

def obtener_df_pca(num_componentes = 19):
    cliente = mf.crear_cliente()
    df_pca = mf.bajar_fichero(cliente, "grupo3/cleaned/pca/final_pca.parquet", "df")
    columnas_componentes = [f"PC{i+1}" for i in range(num_componentes)]
    
    return df_pca[columnas_componentes + ["final"]]