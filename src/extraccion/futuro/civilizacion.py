import pandas as pd
from extraccion import minioFunctions
import numpy as np
from sklearn.neighbors import BallTree

def limpieza_inicial():
    '''
    Realiza la limpieza inicial a partir del dataset de poblaciones original y se sube al minio
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

def civilizacion(df):
    '''
    Calcula para todo un dataframe las distancias (en km) a la civilización más cercana a
    partir de nuestro dataset de poblaciones utilizando la distancia de Haversine,
    previamente habiendo convertido a radianes las coordenadas y creando un índice espacial
    con BallTree de sklearn.
    '''

    cliente = minioFunctions.crear_cliente()
    path = 'grupo3/maps/civilizaciones/poblaciones_clean.parquet'
    df_pobl = minioFunctions.bajar_fichero(cliente, path)

    # Pasar a radianes
    pobl_rad = np.deg2rad(np.array(df_pobl[['lat', 'lon']]))
    inc_rad = np.deg2rad(np.array(df[['lat', 'lon']]))

    # Crea el índice espacial
    tree = BallTree(pobl_rad, metric='haversine')

    # Calcula la distancia a la más cercana
    distancias = tree.query(inc_rad, k=1)[0]

    # 6371 es el radio de la Tierra en km
    distancias = distancias * 6371

    df_final = pd.DataFrame({'lat': df['lat'], 'lon': df['lon'], 'dist_civ': distancias.flatten()})

    print(df_final)

    minioFunctions.preguntar_subida(df_final, f'grupo3/raw/civilizacion/')

    return df_final
