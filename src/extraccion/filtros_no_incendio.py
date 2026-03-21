from extraccion.descartadas import vegetacion2
from . import minioFunctions
import numpy as np
import geopandas as gpd 
from datetime import date

# Filtros encargados de mejorar la precisión y utilidad de datos de no incendio generados

def esIncendio(lat, lon, incendios):
  '''
    Función para determinar si este punto ya se encuentra registrado en el dataset como incendio

    Parámetros:
    - lat: latitud
    - lon: longitud

    Devuelve:
    - Booleano
    '''
  
  esIncendio = ((incendios.lat == lat) & (incendios.lon == lon)).any() 
  return esIncendio


def esAguaUrbano(lat, lon, src, transformer):
  '''
    Función para determinar si este punto se encuentra en el agua o en zona urbana. 
    (Esta función solo sirve para las zonas registradas en Europa)

    Parámetros:
    - lat: latitud
    - lon: longitud
    - src: archivo raster del .tif de vegetación
    - transformer: transformador de coordenadas geográficas

    Devuelve:
    - Booleano
    '''

  aguaUrbano = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34, 33, 36, 37, 38, 39, 40, 41, 42, 43]
  num = vegetacion2.obtenerNumero(lat, lon, src, transformer)
  return (np.isin(num, aguaUrbano)) | (num == 44) | (num == -1) #-1 o 44 significa noData


def puntoValido(lat, lon, parquet, src, transformer):
  '''
    Función para determinar si este punto cumple las condiciones de las funciones anteriores

    Parámetros:
    - lat: latitud
    - lon: longitud
    - src: archivo raster del .tif de vegetación
    - transformer: transformador de coordenadas geográficas

    Devuelve:
    - Booleano
    '''
  
  noIncendio = not esIncendio(lat, lon, parquet)
  #noAguaUrbano = not esAguaUrbano(lat, lon, src, transformer)

  #return noIncendio and noAguaUrbano
  return noIncendio


def filtrarZona(mascarasRegiones, parquetAnio, cliente): #Pasamos la lista de parquets de las mascaras y el parquet del año que queremos
  '''
    Función para filtrar los puntos, según la zona biogeográfica a la que pertenezcan

    Parámetros:
    - mascarasRegiones: lista con las rutas de todas las máscaras de las regiones
    - parquetAnio: parquet con los datos de incendio de un año determinado
    - cliente: cliente MinIO

    Devuelve:
    - Lista con los dataFrames divididos por zona biogeográfica
    '''
  
  parquetsZonas = []
  for mascaraZona in mascarasRegiones:
      zona = minioFunctions.bajar_fichero(cliente, mascaraZona, "gdf")
      gdf = gpd.GeoDataFrame(
          parquetAnio,
          geometry=gpd.points_from_xy(parquetAnio.lon, parquetAnio.lat),
          crs="EPSG:4326"
      )
      zona = zona.to_crs(gdf.crs)
      gdf = gdf.to_crs(zona.crs) #Transforma al sistema de coordenadas de la zona
      mascara = gdf.geometry.within(zona.geometry.iloc[0], align=False) #Crea el filtro de los puntos que pertenecen a la zona estudiada
      gdf_filtrado = gdf[mascara].copy()
      parquetsZonas.append(gdf_filtrado.drop(columns="geometry")) #Devuelve el parquet de esa zona
  return parquetsZonas


def filtrar_zona_eliminar(ruta, df, cliente):
  '''
    Función para filtrar los puntos pertenecientes a una zona determinada

    Parámetros:
    - ruta: ruta de la máscara de la zona
    - df: dataFrame con todos los puntos
    - cliente: cliente MinIO

    Devuelve:
    - dataFrame con los puntos pertenecientes a esa zona
    '''
  
  zona = minioFunctions.bajar_fichero(cliente, ruta,"gdf")
  gdf = gpd.GeoDataFrame(
      df,
      geometry=gpd.points_from_xy(df.lon, df.lat),
      crs="EPSG:4326"
  )
  zona = zona.to_crs(gdf.crs)
  gdf = gdf.to_crs(zona.crs) #Transforma al sistema de coordenadas de la zona
  mascara = gdf.geometry.within(zona.geometry.iloc[0], align=False) #Crea el filtro de los puntos que pertenecen a la zona estudiada
  gdf_zona = gdf[mascara].copy()

  return gdf_zona.drop(columns="geometry") #Devuelve el parquet de la zona


def crearFecha(dia, mes, anio):
  '''
    Función para crear una fecha

    Parámetros:
    - dia, mes y año para la fecha

    Devuelve:
    - fecha en formato string
    '''
  
  fecha = date(anio, mes, dia)
  return fecha.strftime("%Y-%m-%d")