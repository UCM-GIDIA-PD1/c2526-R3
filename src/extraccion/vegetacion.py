import ee
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import asyncio
from . import incendios
import pandas as pd
import time

sem_global = asyncio.Semaphore(10)

load_dotenv()

# Obtener la ruta del json de credenciales desde las variables de entorno
ruta_credenciales = os.getenv("RUTA_CREDENCIALES")

# Autenticación con Google Earth Engine con aviso por si hay fallo
try:
    credentials = ee.ServiceAccountCredentials(
        key_file=ruta_credenciales
    )
    
    ee.Initialize(credentials)
    print("Autenticación exitosa")

except Exception as e:
    print(f"Error al autenticar la API de Earth Engine, revisa la ruta del archivo de credenciales o el nombre del proyecto: {e}")


def quitar_dias(fecha_str):
    '''
    Resta 21 días a la fecha ingresada (en formato string)
    '''
    if isinstance(fecha_str, str):
        fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
    else:
        fecha_obj = fecha_str
        
    menos_21 = fecha_obj - timedelta(days=21)
    
    return menos_21.strftime('%Y-%m-%d')


def calcular_indices(img):
    '''
    Calcula los indices NDVI y NDWI a partir de una imagen
    '''
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    return img.addBands([ndvi, ndwi])

def imagen(punto, fecha):
  '''
  Obtiene la imagen del satélite Copernicus para una ubicacion
  y fecha concretas. Para la fecha utiliza un rango de entre la
  fecha introducida y 21 días antes para evitar que no haya datos,
  y devuelve la mediana de ese rango de fechas, quitando los datos
  con nubes.
  '''
  fecha_fin = fecha
  fecha_ini = quitar_dias(fecha)

  cloud_score = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED')
  umbral_nubes = 0.5

  img = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
            .filterBounds(punto)
            .filterDate(fecha_ini, fecha_fin)
            .linkCollection(cloud_score, ['cs_cdf'])
            .map(lambda img: img.updateMask(img.select('cs_cdf').gte(umbral_nubes)))
            .select(['B3', 'B4', 'B8'])
            .map(calcular_indices)
            .median())
  
  return img



def logica_vegetacion(lat, lon, fecha):
  '''
  Calcula los valores de NDVI y NDWI para una ubicacion y fecha concretas,
  y en el caso de no haber datos en el rango de fechas, devuelve nulos.
  '''
  punto = ee.Geometry.Point([lon, lat])
  img_data = imagen(punto, fecha)

  if len (img_data.bandNames().getInfo()) > 0:

    datos = img_data.select(['NDVI', 'NDWI']).sample(region = punto, scale = 10).first().getInfo()

    return datos['properties']
  else:
    return {'NDVI': np.nan, 'NDWI': np.nan}
  
async def vegetacion(lat, lon, fecha, indice = None):

  async with sem_global:
    
    resultado = await asyncio.to_thread(logica_vegetacion, lat, lon, fecha)
    if indice is not None:
      print(f"Vegetación {indice} extraida.")
    return resultado


async def df_vegetacion(filepath, limit = 20, fecha_ini = None, fecha_fin = None):
  
  ini = time.time()

  print("Comenzando extracción...")

  fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)
  tareas = [
        vegetacion(row['lat_mean'], row['lon_mean'], row['date_first'], indice = i)
        for i, row in enumerate(fires.head(limit).to_dict('records'))
    ]
  resultados = await asyncio.gather(*tareas)
  final_df = pd.DataFrame(resultados)

  fin = time.time()

  print(f"Extraidas {limit} filas de vegetación en {fin - ini:.2f} segundos.")
  print(final_df.head(limit))
  return final_df
  