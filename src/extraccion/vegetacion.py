import ee
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv
import asyncio
from extraccion import minioFunctions
from . import interrupcion
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
    Resta 30 días a la fecha ingresada (en formato string)
    
    :param fecha_str: Fecha en formato string (se tomarán los primeros 10 caracteres)
    :return str: Nueva fecha restando 30 días en formato '%Y-%m-%d'
    '''
    fecha_str = str(fecha_str)[:10]
    fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
        
    menos_30 = fecha_obj - timedelta(days=30)
    
    return menos_30.strftime('%Y-%m-%d')


def calcular_indices(img):
    '''
    Funcion que calcula los indices NDVI y NDWI a partir de una imagen de satélite con las bandas necesarias (B3, B4, B8)
    
    :param img: Objeto ee.Image del cual se calcularán los índices
    :return ee.Image: Imagen original con las nuevas bandas 'NDVI' y 'NDWI' añadidas
    '''
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    return img.addBands([ndvi, ndwi])

def imagen(punto, fecha):
  '''
  Obtiene la primera imagen de un satélite teniendo en cuenta que no haya nubes (Umbral de 30 días).
  Si la fecha es futura, usa la fecha actual como límite final.

  :param punto: Objeto ee.Geometry.Point del cual se quiere obtener la imagen
  :param fecha: Fecha en formato string (se tomarán los primeros 10 caracteres)
  :return ee.Image: Imagen con las bandas 'NDVI' y 'NDWI' añadidas
  '''
  fecha_str = str(fecha)[:10]
  target_date = datetime.strptime(fecha_str, '%Y-%m-%d').date()
  today = datetime.now().date()
  
  if target_date > today:
      fecha_fin = today.strftime('%Y-%m-%d')
  else:
      fecha_fin = fecha_str
      
  fecha_ini = quitar_dias(fecha_fin)

  cloud_score = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED')
  umbral_nubes = 0.3

  img = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
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

  :param lat: Latitud del punto
  :param lon: Longitud del punto
  :param fecha: Fecha para la cual se quieren extraer los datos
  :return dict: diccionario con los valores de NDVI y NDWI o NaN si no se pudieron obtener datos
  '''

  try:
    punto = ee.Geometry.Point([lon, lat])
    img_data = imagen(punto, fecha)

    datos = img_data.select(['NDVI', 'NDWI']).sample(region = punto, scale = 10).getInfo()

    if datos is None or len(datos.get('features', [])) == 0:
      print(f"Advertencia: No se encontraron datos para Lat: {lat}, Lon: {lon}")
      return{'NDVI':np.nan, 'NDWI':np.nan}
    else:
      return {'NDVI':datos['features'][0]['properties']['NDVI'], 'NDWI':datos['features'][0]['properties']['NDWI']}
    
  except Exception as e:
    print(f"Error al obtener datos para Lat: {lat}, Lon: {lon}, Fecha: {fecha}. Detalles del error: {e}")
    return{'NDVI':np.nan, 'NDWI':np.nan}
  
async def vegetacion(lat, lon, fecha, indice = None):

  '''
  Obtiene los indices de vegetacion para una ubicacion y fecha

  :param lat: Latitud del punto
  :param lon: Longitud del punto
  :param fecha: Fecha para la cual se quieren extraer los datos
  :param indice: indice del punto en el DataFrame (opcional, para depurar)
  :return dict: diccionario con los valores de NDVI y NDWI para la ubicación y fecha dadas, incluyendo lat, lon y date
  '''

  async with sem_global:
    resultado = await asyncio.to_thread(logica_vegetacion, lat, lon, fecha)
    if indice is not None:
      print(f"Vegetación {indice} extraida.")
    
    resultado['lat'] = lat
    resultado['lon'] = lon
    resultado['date'] = fecha
    
    return resultado


async def df_vegetacion(fires, limit = 20, fecha_ini = None, fecha_fin = None, pipeline = False, anio = None):

  '''
  Obtiene un DataFrame con los índices de vegetación para los incendios en un rango de fechas

  :param fires: DataFrame con los incendios con columnas 'lat', 'lon', y 'date'
  :param limit: número de incendios a procesar (por defecto 20, -1 para procesar todos)
  :param fecha_ini: fecha inicial del rango (por defecto None)
  :param fecha_fin: fecha final del rango (por defecto None)
  :param pipeline: si es true se automatiza la subida a Minio sin preguntar (por defecto False)
  :param anio: Año para subir el archivo a Minio automáticamente
  :return pd.DataFrame: DataFrame final con los índices de vegetación para los incendios procesados
  '''
    
  ini = time.time()
  print("Comenzando extracción...")

  if limit == -1:
      rows = fires.to_dict('records')
  else:
      rows = fires.head(limit).to_dict('records')
  
  tareas = [
      vegetacion(
          row['lat'],
          row['lon'],
          row['date'],
          indice=i
      )
      for i, row in enumerate(rows)
  ]

  resultados = []
  try:
      for tarea in asyncio.as_completed(tareas):
          try:
              resultados.append(await tarea)
          except asyncio.CancelledError:
              print("\n Interrupción detectada. Guardando resultados parciales...")
              if resultados:
                  final_df = pd.DataFrame(resultados)
                  interrupcion.guardar_parcial(final_df, prefijo="vegetacion_parcial")
              else:
                  print("No hay datos parciales para guardar.")
              for t in tareas:
                  if not t.done():
                      t.cancel()
              raise
  except KeyboardInterrupt:
      print("\n Interrupción detectada. Guardando resultados parciales...")
      if resultados:
          final_df = pd.DataFrame(resultados)
          interrupcion.guardar_parcial(final_df, prefijo="vegetacion_parcial")
      else:
          print("No hay datos parciales para guardar.")
      raise

  final_df = pd.DataFrame(resultados)

  fin = time.time()
  print(f"Extraídas {len(final_df)} filas de vegetación en {fin - ini:.2f} segundos.")
  print(final_df.head(limit))

    
  if pipeline:
      assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
      cliente = minioFunctions.crear_cliente()
      minioFunctions.subir_fichero(cliente, f"grupo3/raw/Vegetacion/Vegetacion_{anio}.parquet", final_df)
  else:
      minioFunctions.preguntar_subida(final_df, "grupo3/raw/Vegetacion/")
  return final_df