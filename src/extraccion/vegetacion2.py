'''Esta función se corresponde con la extracción de un número identificado 
con un tipo de vegetación en un archivo, zona urbana o agua. Se aplica rasterización, dada
una latitud y longitud''' 

import rasterio
import pandas as pd
from pyproj import Transformer

def obtenerNumero(lat, lon, src, data):
    transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
    x, y = transformer.transform(lon, lat)
    row, col = src.index(x, y)
    return data[row, col]

def entorno(lat, lon, src, data, df):
    num = obtenerNumero(lat, lon, src, data)
    if num < 0:
        num = 44
    return {"entorno": df.loc[num]["Column6"]}

def abrirMapa(): 
    return rasterio.open("src/mapa.tif") 

def cerrarMapa(src): 
    src.close()


