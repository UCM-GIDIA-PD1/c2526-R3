import ee
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import asyncio
from . import incendios, minioFunctions
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
    # if isinstance(fecha_str, str):
    #     fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
    # else:
    #     fecha_obj = fecha_str

    fecha_str = str(fecha_str)[:10]
    fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
        
    menos_21 = fecha_obj - timedelta(days=21)
    
    return menos_21.strftime('%Y-%m-%d')


def calcular_indices(img):
    '''
    Funcion que calcula los indices NDVI y NDWI a partir de una imagen de satélite con las bandas necesarias (B3, B4, B8)
    '''
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    return img.addBands([ndvi, ndwi])

def imagen(punto, fecha):
  '''
  Obtiene la imagen del satélite Copernicus en un rango de 21 dias ignorando los datos con nubes

  Parametros:
  - punto: ee.Geometry.Point con la ubicacion

  Devuelve:
  - img: ee.Image con los datos de la imagen procesada (mediana de las imagenes disponibles en el rango de fechas)
  '''
  fecha = str(fecha)[:10]
  
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
  Extrae los índices de vegetación (NDVI y NDWI) para una ubicación y fecha

  Devuelve:
  - resultado: diccionario con los valores de NDVI y NDWI o NaN si no se pudieron obtener datos
  '''

  try:
    punto = ee.Geometry.Point([lon, lat])
    img_data = imagen(punto, fecha)

    datos = img_data.select(['NDVI', 'NDWI']).sample(region = punto, scale = 10).getInfo()

    if datos is None or 'properties' not in datos:
      print(f"Advertencia: No se encontraron datos para Lat: {lat}, Lon: {lon}")
      return{'NDVI':np.nan, 'NDWI':np.nan}
    else:
      return datos['properties']
    
  except Exception as e:
    print(f"Error al obtener datos para Lat: {lat}, Lon: {lon}, Fecha: {fecha}. Detalles del error: {e}")
    return{'NDVI':np.nan, 'NDWI':np.nan}
  
async def vegetacion(lat, lon, fecha, indice = None):

  '''
  Obtiene los indices de vegetacion para una ubicacion y fecha

  Parámetros:
  - indice: indice del punto en el DataFrame (opcional, para depurar)

  Devuelve:
  - resultado: diccionario con los valores de NDVI y NDWI para la ubicación y fecha dadas
  '''

  async with sem_global:
    resultado = await asyncio.to_thread(logica_vegetacion, lat, lon, fecha)
    if indice is not None:
      print(f"Vegetación {indice} extraida.")
    
    resultado['lat'] = lat
    resultado['lon'] = lon
    resultado['date'] = fecha
    
    return resultado


async def df_vegetacion(fires, limit = 20, fecha_ini = None, fecha_fin = None):

  '''
  Obtiene un DataFrame con los índices de vegetación para los incendios en un rango de fechas

  Parámetros:
  - fires: DataFrame con los incendios con columnas 'lat_mean', 'lon_mean', y 'date_first'
  - limit: número de incendios a procesar (por defecto 20)
  - fecha_ini: fecha inicial del rango (por defecto None)
  - fecha_fin: fecha final del rango (por defecto None)
  
  Devuelve:
  - final_df: DataFrame con los índices de vegetación para los incendios procesados
  '''
  
  ini = time.time()

  print("Comenzando extracción...")

  #fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)
  
  #cliente = minioFunctions.crear_cliente()
  #fires = minioFunctions.bajar_fichero(cliente, filepath, "df")

  if limit == -1:
    rows = fires.to_dict('records')
  else:
    rows = fires.head(limit).to_dict('records')
  tareas = [
      vegetacion(
          row['lat_mean'],
          row['lon_mean'],
          row['date_first'],
          indice=i
      )
      for i, row in enumerate(rows)
  ]
  resultados = await asyncio.gather(*tareas)
  final_df = pd.DataFrame(resultados)

  fin = time.time()

  
  print(f"Extraídas {len(final_df)} filas de vegetación en {fin - ini:.2f} segundos.")
  print(final_df.head(limit))

  minioFunctions.preguntar_subida(final_df, "grupo3/raw/Vegetacion/")

  return final_df
