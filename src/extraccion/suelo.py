'''Esta función se corresponde con la extracción de un valor continuo de propiedades 
del suelo (ej. retención de agua, SOC) en un archivo .tif. Se aplica rasterización, dada
una latitud y longitud''' 

import rasterio
import pandas as pd
import numpy as np # Importante para manejar los valores nulos (NaN)
from pyproj import Transformer

import time
import os
import fsspec
from rasterio.windows import Window
from dotenv import load_dotenv
from . import incendios
import asyncio
from extraccion import minioFunctions
from pathlib import Path

def obtenerValorSuelo(lat, lon, src, transformer):
    '''
    Extrae el valor del pixel correspondiente a una coordenada dentro de un raster de suelo.
    :param lat: latitud
    :param lon: longitud
    :param src: nuestro archivo .tif sobre el que consultaremos 
    :param transformer: transformador de lon/lat al mismo sistema de coordenadas de nuestro archivo
    '''
    x, y = transformer.transform(lon, lat) #Transformamos longitud y latitud al sistema de coordenadas del ráster
    row, col = src.index(x, y) #Convertirmos las coordenadas a ráster
                
    window = Window(col, row, 1, 1) #Creamos una ventana para que solamente se lea un píxel y no todo el .tif
    data = src.read(1, window=window)
            
    if data.size > 0:
        val = data[0, 0]
        if val < 0: 
            return np.nan 
        return float(val)
    else:
        return np.nan

def lista_entorno_suelo(lista_puntos, ruta_tif): 

    """
    Mapea una lista de coordenadas a sus respectivos valores del suelo.
    Abre una conexion al raster alojado en MinIO y extrae directamente el valor numérico.
    :param lista_puntos: lista con los puntos (latitud, longitud) 
    :param ruta_tif: ruta a minio de nuestro archivo
    """
    load_dotenv()
    
    minio_config = {
        "AWS_S3_ENDPOINT": "minio.fdi.ucm.es",
        "AWS_HTTPS": "YES",
        "AWS_VIRTUAL_HOSTING": "FALSE",
        "GDAL_HTTP_UNSAFESSL": "YES",
    }
    ak, sk = minioFunctions.importar_keys()

    with rasterio.Env(**minio_config,
                      aws_access_key_id=ak,
                      aws_secret_access_key=sk):
        
        # Leemos directamente la ruta pasada por parámetro
        with rasterio.open(ruta_tif) as src:
            transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            lista_suelo = []
            
            for i, (lon, lat) in enumerate(lista_puntos):
                val = obtenerValorSuelo(lat, lon, src, transformer)
                lista_suelo.append(val)
                print(f"Dato de suelo {i} extraído")
                
            return lista_suelo

async def df_suelo(fires, variable_name="water_capacity", ruta_tif="/vsis3/pd1/grupo3/mapa/suelo.tif", limit=20):

    """
    Se extraen los datos de una variable de suelo específica para una serie de incendios
    """
    
    ini = time.time()
    
    if limit != -1:
        fires = fires.head(limit)   
    
    lista_puntos = list(zip(fires['lon_mean'], fires['lat_mean']))

    # Ya no llamamos a pd.read_csv para mapear, pasamos la ruta del TIF directamente
    lista_res = await asyncio.to_thread(lista_entorno_suelo, lista_puntos, ruta_tif)
    
    fires = fires[['lat_mean','lon_mean','date_first']].copy().reset_index(drop = True)

    # Creamos la columna dinámicamente según la variable que estemos procesando
    final_df = pd.DataFrame(lista_res, columns=[variable_name])
    final_df = pd.concat([final_df, fires], axis = 1)
    final_df = final_df.rename(columns={'lat_mean':'lat', 'lon_mean':'lon', 'date_first':'date'})

    print(f"Finalizado en {time.time() - ini:.2f}s")
    print(final_df.head(limit))

    # Guardamos en una carpeta nueva dentro de nuestro bucket raw en MinIO
    minioFunctions.preguntar_subida(final_df, f"grupo3/raw/Suelo_{variable_name}/")

    return final_df


def info_tif():
    ruta_tif = Path(__file__).resolve().parent.parent.parent / "data/NEW_RUSLE2016.tfw"

    print(f"Inspeccionando archivo: {ruta_tif}\n" + "-"*40)

    with rasterio.open(ruta_tif) as src:
        print(f"Bandas: {src.count}")
        print(f"Descripciones: {src.descriptions}")
    
        print("Metadatos globales del archivo:")
        for clave, valor in src.tags().items():
            print(f"   -> {clave}: {valor}")
        
        print("Metadatos específicos por banda:")
        for i in range(1, src.count + 1):
            tags_banda = src.tags(i)
            if tags_banda:
                print(f"Banda {i}: {tags_banda}")
            else:
                print(f"Banda {i}: (Sin metadatos adicionales)")