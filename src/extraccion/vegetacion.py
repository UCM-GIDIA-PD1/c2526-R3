import ee
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

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
    Resta 21 días a la fecha ingresada
    '''
    fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
    menos_21 = fecha_obj - timedelta(days=21)
    return menos_21.strftime('%Y-%m-%d')

def quita_nubes(image):
  '''
  Elimina nubes de la imagen
  '''
  qa = image.select('QA60')

  cloud_bit_mask = 1 << 10
  cirrus_bit_mask = 1 << 11

  mask = (
      qa.bitwiseAnd(cloud_bit_mask)
      .eq(0)
      .And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
  )

  return image.updateMask(mask).divide(10000)

def calcular_indices(img):
    '''
    Calcula los indices NDVI y NDWI a partir de una imagen
    '''
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    return img.addBands([ndvi, ndwi])

def imagen(lat, lon, fecha):
  '''
  Obtiene la imagen del satélite Copernicus para una ubicacion
  y fecha concretas. Para la fecha utiliza un rango de entre la
  fecha introducida y 21 días antes para evitar que no haya datos,
  y devuelve la mediana de ese rango de fechas.
  '''
  punto = ee.Geometry.Point([lon, lat])
  fecha_fin = fecha
  fecha_ini = quitar_dias(fecha)
  imagen_ee_object = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
            .filterBounds(punto)
            .filterDate(fecha_ini, fecha_fin)
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
            .map(quita_nubes)
            .map(calcular_indices)
            .median())
  return punto, imagen_ee_object



def vegetacion(lat, lon, fecha):
  '''
  Calcula los valores de NDVI y NDWI para una ubicacion y fecha concretas,
  y en el caso de no haber datos en el rango de fechas, devuelve nulos.
  '''
  punto, img_data = imagen(lat, lon, fecha)

  if len (img_data.bandNames().getInfo()) > 0:
    datos = img_data.select(['NDVI', 'NDWI']).sample(punto, 10).first().getInfo()

    return datos['properties']
  else:
    return {'NDVI': np.nan, 'NDWI': np.nan}