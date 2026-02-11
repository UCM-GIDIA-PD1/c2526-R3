import ee
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()
proyecto = os.getenv("PROYECTO_JUANAN")

ee.Authenticate()
ee.Initialize(project=proyecto)

def quitar_dias(fecha_str):
    fecha_obj = datetime.strptime(fecha_str, '%Y-%m-%d')
    menos_21 = fecha_obj - timedelta(days=21)
    return menos_21.strftime('%Y-%m-%d')

def quita_nubes(image):
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
    ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI')
    ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI')
    return img.addBands([ndvi, ndwi])

def imagen(lat, lon, fecha):
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
  punto, img_data = imagen(lat, lon, fecha)

  if len (img_data.bandNames().getInfo()) > 0:
    datos = img_data.select(['NDVI', 'NDWI']).sample(punto, 10).first().getInfo()

    return datos['properties']
  else:
    return {'NDVI': np.nan, 'NDWI': np.nan}