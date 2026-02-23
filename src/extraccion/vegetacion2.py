'''Esta función se corresponde con la extracción de un número identificado 
con un tipo de vegetación en un archivo, zona urbana o agua. Se aplica rasterización, dada
una latitud y longitud''' 

import rasterio
import pandas as pd
from pyproj import Transformer

import time
import os
import fsspec
from rasterio.windows import Window
from dotenv import load_dotenv
from . import incendios
import asyncio
from extraccion import minioFunctions

def obtenerNumero(lat, lon, src, transformer):
    x, y = transformer.transform(lon, lat)
    row, col = src.index(x, y)
                
    window = Window(col, row, 1, 1)
    data = src.read(1, window=window)
            
    if data.size > 0:
        num = data[0, 0]
        if num < 0: num = 44
    else:
        num = -1
    return num

def lista_entorno(lista_puntos, df_vegetacion): 
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
        
        with rasterio.open("/vsis3/pd1/grupo3/mapa/mapa.tif") as src:
            transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            lista_vegetacion = []
            
            for i, (lon, lat) in enumerate(lista_puntos):
                num = obtenerNumero(lat, lon, src, transformer)
                if num > -1:
                    lista_vegetacion.append(df_vegetacion.loc[num]["Column6"])
                else:
                    lista_vegetacion.append("Sin datos")
                
                print(f"Dato {i} extraído")
            return lista_vegetacion

async def df_vegetacion2(fires, limit=20, fecha_ini=None, fecha_fin=None):
    
    ini = time.time()
    ak, sk = minioFunctions.importar_keys()

    df_aux = pd.read_csv("s3://pd1/grupo3/mapa/mapa_vegetacion.csv", 
                         storage_options={
                             "key": ak,
                             "secret": sk,
                             "client_kwargs": {"endpoint_url": "https://minio.fdi.ucm.es", "verify": False}
                         })

    #fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)
    fires = fires.head(limit)
    
    lista_puntos = list(zip(fires['lon_mean'], fires['lat_mean']))


    lista_res = await asyncio.to_thread(lista_entorno, lista_puntos, df_aux)
    fires = fires[['lat_mean','lon_mean','date_first']].copy().reset_index(drop = True)

    final_df = pd.DataFrame(lista_res, columns=["vegetacion2"])
    final_df = pd.concat([final_df, fires], axis = 1)
    final_df = final_df.rename(columns={'lat_mean':'lat', 'lon_mean':'lon', 'date_first':'date'})

    print(f"Finalizado en {time.time() - ini:.2f}s")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Vegetacion2/")

    return final_df
