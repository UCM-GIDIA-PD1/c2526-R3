from extraccion import minioFunctions as mf
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler 

def bajar_df_final():
    cliente = mf.crear_cliente()
    df = mf.bajar_fichero(cliente, "grupo3/cleaned/final.parquet", "df")
    return df


def limpieza(df):
    df = df.drop(['date_last'], axis = 1)

    df1 = df.copy()
    df2 = df.copy()
    df3 = df.copy()
    df4 = df.copy()

    df1 = df1.drop(['temp_min', 'temp_max', 'sunshine_seconds', 'evapotranspiration', 'NDWI', 'elevacion_centro', 'porcentaje', 'wind_gusts_max'], axis = 1)
    df2 = df2.drop(['temp_min', 'temp_mean', 'sunshine_seconds', 'radiation', 'NDVI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)
    df3 = df3.drop(['temp_min', 'temp_mean', 'sunshine_seconds', 'evapotranspiration', 'NDVI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)
    df4 = df4.drop(['temp_min', 'temp_max', 'sunshine_seconds', 'evapotranspiration', 'NDWI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)

    return [df1.dropna(), df2.dropna(), df3.dropna(), df4.dropna()]

def limpieza_coordenadas():
    '''
    XGBoost le está dando demasiada importancia a las variables de latitud y longitud, por lo que vamos
    a probar eliminarlas para intentar que tenga más en cuenta las variables climatológicas y nuestro
    modelo prediga en base a las condiciones y no a la localización.
    '''
    df = bajar_df_final()
    
    df_return = df.drop(columns=["lat", "lon"], errors="ignore")
    
    # Subimos a MinIO
    cliente = mf.crear_cliente()
    mf.subir_fichero(cliente, "grupo3/cleaned/final_lat_lon.parquet", df_return)

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