import pandas as pd
import numpy as np
from extraccion import minioFunctions as mf
import limpieza as lp
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler 

def pca(df, subir_a_minio=True):
    '''
    Aplicamos PCA a un dataset para reducir la dimensionalidad

    :param df: dataframe con las características y la variable objetivo
    :param subir_a_minio: booleano para decidir si subir el dataframe a minio
    
    :return df_pca: dataframe con las componentes principales y la variable objetivo
    :return df_metricas: dataframe con las métricas (de momento solo he incluído la varianza).
    '''
    
    # Separamos las características y variable objetivo
    X = df.drop(columns=["final"])
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

    if subir_a_minio:
        print("Creado el dataframe con las componentes principales.")
        mf.preguntar_subida(df_pca, "grupo3/cleaned/pca/")

    #Obtenemos las métricas del pca para su posterior análisis y las subimos a minio como parquet
    varianza = pca.explained_variance_ratio_
    
    df_metricas = pd.DataFrame({
        'Componente': [f'PC{i+1}' for i in range(len(varianza))],
        'Varianza_Explicada': varianza,
    })

    if subir_a_minio:
        print("Creado el dataframe con las metricas.")
        mf.preguntar_subida(df_metricas, "grupo3/cleaned/pca/")

    return df_pca, df_metricas

def obtener_df_pca(num_componentes):
    '''
    Obtenemos el dataframe con las componentes principales

    :param num_componentes: número de componentes principales a obtener
    :return df_pca: dataframe
    '''
    cliente = mf.crear_cliente()
    df_pca = mf.bajar_fichero(cliente, "grupo3/cleaned/pca/final_pca.parquet", "df")
    columnas_componentes = [f"PC{i+1}" for i in range(num_componentes)]
    
    return df_pca[columnas_componentes + ["final"]]

def tranformar_date():
    '''
    Extrae de la columna "date" el día y lo transforma a un formato cíclico para 
    que el modelo pueda entenderlo mejor (usando senos y cosenos). Posteriormente lo sube a MinIO
    '''

    df = lp.bajar_df_final()
    print(df.columns)

    #Extraemos el día y lo transformamos con senos y cosenos a formato cíclico
    df["date"] = pd.to_datetime(df["date"])
    dias = df["date"].dt.dayofyear
    df['dia_sin'] = np.sin(2 * np.pi * dias / 365)
    df['dia_cos'] = np.cos(2 * np.pi * dias / 365)

    # Subimos a minio
    cliente = mf.crear_cliente()
    mf.subir_fichero(cliente, "grupo3/cleaned/MINI.parquet", df)

def obtener_df_date_transformado():
    '''
    Obtenemos el dataframe con la transformación de la fecha
    
    :return df_date_transformado: dataframe 
    '''
    cliente = mf.crear_cliente()
    df_date_transformado = mf.bajar_fichero(cliente, "grupo3/cleaned/final_date_transformado.parquet", "df")
    
    return df_date_transformado

