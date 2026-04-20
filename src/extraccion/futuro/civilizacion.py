import pandas as pd
from extraccion import minioFunctions
import numpy as np
from sklearn.neighbors import BallTree
import asyncio

def limpieza_inicial():
    '''
    Realiza la limpieza inicial a partir del dataset de poblaciones original y se sube al minio
    
    :return None:
    '''
    cliente = minioFunctions.crear_cliente()
    path = 'grupo3/maps/civilizaciones/poblaciones_mas_de_1000.csv'
    df = minioFunctions.bajar_csv(cliente, path, ';')

    df[['lat', 'lon']] = df['Coordinates'].str.split(',', expand=True)

    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)

    df = df[['Geoname ID', 'Name', 'Population', 'lat', 'lon']]

    df = df[df['Population'] > 150]

    minioFunctions.subir_fichero(cliente, 'grupo3/maps/civilizaciones/poblaciones_clean.parquet', df)

def calcular_distancias(df_pobl, df_inc):
    # Pasar a radianes
    pobl_rad = np.deg2rad(np.array(df_pobl[['lat', 'lon']]))
    inc_rad = np.deg2rad(np.array(df_inc[['lat', 'lon']]))

    # Crea el índice espacial
    tree = BallTree(pobl_rad, metric='haversine')

    # Calcula la distancia a la más cercana
    distancias = tree.query(inc_rad, k=1)[0]

    # 6371 es el radio de la Tierra en km
    return distancias * 6371

async def civilizacion(df, limit=20, fecha_ini=None, fecha_fin=None, pipeline=False, anio=None):
    '''
    Calcula para todo un dataframe las distancias (en km) a la civilización más cercana a
    partir de nuestro dataset de poblaciones utilizando la distancia de Haversine,
    previamente habiendo convertido a radianes las coordenadas y creando un índice espacial
    con BallTree de sklearn.
    
    :params df: DataFrame con los puntos
    :params limit: Límite de filas
    :params fecha_ini: Fecha de inicio
    :params fecha_fin: Fecha de fin
    :param pipeline: si es true se automatiza la subida a Minio sin preguntar (por defecto False)
    :param anio: Año para subir el archivo a Minio automáticamente
    :return pd.DataFrame: DataFrame final
    '''
    fin_none = fecha_fin is None
    ini_none = fecha_ini is None

    if not fin_none and not ini_none: 
        df = df[df['date'].between(fecha_ini, fecha_fin)]

    if limit != -1:
        df = df.head(limit) 

    df = df.reset_index(drop=True)

    cliente = await asyncio.to_thread(minioFunctions.crear_cliente)
    path = 'grupo3/maps/civilizaciones/poblaciones_clean.parquet'
    df_pobl = await asyncio.to_thread(minioFunctions.bajar_fichero, cliente, path)

    try:
        distancias = await asyncio.to_thread(calcular_distancias, df_pobl, df)

    except asyncio.CancelledError:
        print("\n Interrupción detectada (Tarea cancelada). No hay datos parciales para guardar en civilizacion.")
        raise
    except KeyboardInterrupt:
        print("\n Interrupción detectada por teclado. No hay datos parciales para guardar en civilizacion.")
        raise

    df_final = pd.DataFrame({'lat': df['lat'], 'lon': df['lon'], 'dist_civ': distancias.flatten()})

    print(df_final)

    if pipeline:
        assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
        cliente = minioFunctions.inicializar_cliente()
        minioFunctions.subir_fichero(cliente, df_final, f"grupo3/raw/civilizacion/civilizacion_{anio}.parquet")
    else:
        await asyncio.to_thread(minioFunctions.preguntar_subida, df_final, f'grupo3/raw/civilizacion/')

    return df_final