'''Esta función se corresponde con la extracción de un número identificado 
con un tipo de vegetación en un archivo, zona urbana o agua. Se aplica rasterización, dada
una latitud y longitud''' 

import rasterio
import pandas as pd
from pyproj import Transformer

import os
import fsspec
import rasterio
from rasterio.windows import Window
from dotenv import load_dotenv

# def obtenerNumero(lat, lon, src, data):
#     transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
#     x, y = transformer.transform(lon, lat)
#     row, col = src.index(x, y)
#     return data[row, col]

# def entorno(lat, lon, src, data, df):
#     num = obtenerNumero(lat, lon, src, data)
#     if num < 0:
#         num = 44
#     return {"entorno": df.loc[num]["Column6"]}

# def abrirMapa(): 
#     return rasterio.open("src/mapa.tif") 

# def cerrarMapa(src): 
#     src.close()

def entorno(lat, lon):
    load_dotenv()

    df = pd.read_csv(
        "s3://pd1/grupo3/mapa/mapa_vegetacion.csv",
        storage_options={
            "key": os.getenv("AWS_ACCESS_KEY_ID"),
            "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "client_kwargs": {
                "endpoint_url": "https://minio.fdi.ucm.es",
                "verify": False  #Importante porque el acceso necesitaría sino verificación
            }
        }
    )

    minio_config = {
    "AWS_S3_ENDPOINT": "minio.fdi.ucm.es",  # Se pone SIN https://
    "AWS_HTTPS": "YES",                     # Aquí indicamos que usa HTTPS
    "AWS_VIRTUAL_HOSTING": "FALSE",         
    "GDAL_HTTP_UNSAFESSL": "YES"           
    }
   
    with rasterio.Env(**minio_config):
        with rasterio.open("/vsis3/pd1/grupo3/mapa/mapa.tif") as src:
            transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            x, y = transformer.transform(lon, lat)
            row, col = src.index(x, y)
            window = Window(col, row, 1, 1)
            num = src.read(1, window=window)[0, 0]
            if num < 0:
                num = 44
            return df.loc[num]["Column6"]





