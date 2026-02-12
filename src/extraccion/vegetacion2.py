'''Esta función se corresponde con la extracción de un número identificado 
con un tipo de vegetación en un archivo, zona urbana o agua. Se aplica rasterización, dada
una latitud y longitud''' 

import rasterio
import pandas as pd
from pyproj import Transformer

def obtenerNumero(lat, lon):
  with rasterio.open("/content/mapa.tif") as src:
      #Tranformador de coordenadas, src.crs es el sistema de coordenadas del ráster
      transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True) 
      x, y = transformer.transform(lon, lat) #Convierte la coordenada (lon, lat) al sistema del raster
      row, col = src.index(x, y) #Convierte en fila y columna del ráster
      num = src.read(1)[row, col] #Lee la primera banda del ráster, y extrae el valor del píxel
      return num - 1

def entorno(lat, lon):
  num = obtenerNumero(lat, lon)
  df = pd.read_excel("/content/mapa_vegetacion.xlsx") #Aquí tenemos guardadas las distintas clasificaciones
  return {"entorno" : df.iloc[num]["Column6"]}