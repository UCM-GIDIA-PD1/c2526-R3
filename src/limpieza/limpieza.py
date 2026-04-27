from extraccion import minioFunctions as mf
import pandas as pd

def bajar_df_final(clasificacion=True):
    cliente = mf.crear_cliente()
    if clasificacion:
        df = mf.bajar_fichero(cliente, "grupo3/cleaned/MINI.parquet", "df")
    else:
        df = mf.bajar_fichero(cliente, "grupo3/cleaned/MI_nuevas.parquet", "df")
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

def mostrar_nulos(df):
    """
    Retorna un resumen de las columnas que tienen nulos y cuántos tienen.
    """

    if df is None:
        return None
    
    nulos = df.isna().sum()
    columnas_con_nulos = nulos[nulos > 0]
    
    if columnas_con_nulos.empty:
        return None
    
    return columnas_con_nulos

def limpieza_nulos(df, columnas=None, pipeline = False, anio = None):
    """
    Borra filas con valores nulos. 
    Si 'columnas' es una lista, solo mira nulos en esas columnas.
    Si 'columnas' es None o vacía, mira en todo el DataFrame.
    """

    if df is None:
        return None
    
    df_temp = df.copy()
    
    if columnas:
        columnas_validas = [c for c in columnas if c in df_temp.columns]
        print(f"Filtrando nulos en columnas: {columnas_validas}")
        df_temp = df_temp.dropna(subset=columnas_validas)
    else:
        print("Filtrando nulos en todas las columnas del DataFrame")
        df_temp = df_temp.dropna()

    if pipeline:
        assert anio is not None, "Si pipeline es True, se debe proporcionar un año para subir el archivo a MinIO."
        cliente = mf.crear_cliente()
        mf.subir_fichero(cliente, f"grupo3/cleaned/final_cleaned_{anio}.parquet", df_temp)
    else:
        mf.preguntar_subida(df_temp, "grupo3/cleaned/")
        
    return df_temp

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


def obtener_df_pca(num_componentes):
    cliente = mf.crear_cliente()
    df_pca = mf.bajar_fichero(cliente, "grupo3/cleaned/pca/final_pca.parquet", "df")
    columnas_componentes = [f"PC{i+1}" for i in range(num_componentes)]
    
    return df_pca[columnas_componentes + ["final"]]