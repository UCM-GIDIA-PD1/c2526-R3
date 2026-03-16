from extraccion import minioFunctions as mf
import pandas as pd

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