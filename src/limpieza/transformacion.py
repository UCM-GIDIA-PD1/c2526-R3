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

def relacionar_variables(df):
    '''
    Relacionamos las variables entre sí para crear nuevas características
    que puedan ser útiles para el modelo.
    
    :param df: dataframe con las características originales
    :return df_relacionadas: dataframe con las nuevas características
    '''
    df_relacionadas = df.copy()

    # Variables de vegetación
    df_relacionadas['vegetacion'] = df_relacionadas['NDVI'] / (df_relacionadas['NDWI'] + 0.01)
    print("Transformada vegetación")
    
    # Variables hídricas
    df_relacionadas["hídricas"] = df_relacionadas["evapotranspiration"] - df_relacionadas["precipitation"]
    print("Transformada hídrica")

    # Variables térmicas
    df_relacionadas["térmicas"] = df_relacionadas["temp_max"] - df_relacionadas["temp_min"]
    print("Transformada térmica")

    # Regla del 30-30-30 (indica riesgo extremo de incendio)
    df_relacionadas["riesgo30"] = (df_relacionadas["temp_max"] * df_relacionadas["wind_speed_max"]) / (df_relacionadas["humidity_mean"] + 1)
    df_relacionadas["regla30"] = ( # Columna booleana que indica si se cumple la regla
        (df_relacionadas["temp_max"] > 30) & 
        (df_relacionadas["wind_speed_max"] > 30) & 
        (df_relacionadas["humidity_mean"] < 30)
    )
    print("Transformada 30-30-30")

    # Variable de viento
    df_relacionadas["viento"] = df_relacionadas["wind_gusts_max"] * df_relacionadas["wind_speed_max"]
    print("Transformada viento")
    
    # Variables humanas
    df_relacionadas["humanas"] = df_relacionadas["NDVI"] / (df_relacionadas["dist_civ"] + 1)
    print("Transformada humana")

    # Variables topográficas
    df_relacionadas["topográficas"] = df_relacionadas["soil_temp"] - df_relacionadas["temp_mean"] 
    print("Transformada topográfica")

    # Variables temporales
    df_relacionadas["date"] = pd.to_datetime(df_relacionadas["date"])
    df_relacionadas["mes"] = df_relacionadas["date"].dt.month
    print("Transformada temporal")

    return df_relacionadas