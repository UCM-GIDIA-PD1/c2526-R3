import pandas as pd
from . import vegetacion2
from . import minioFunctions
import numpy as np
import geopandas as gpd 
from datetime import date

def esIncendio(lat, lon, incendios):
  esIncendio = ((incendios.lat_mean == lat) & (incendios.lon_mean == lon)).any() #Devuelve si alguna fila cumple esta propiedad
  return esIncendio

def esAguaUrbano(lat, lon, src, transformer):
  aguaUrbano = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34, 33, 36, 37, 38, 39, 40, 41, 42, 43]
  num = vegetacion2.obtenerNumero(lat, lon, src, transformer)
  return (np.isin(num, aguaUrbano)) | (num == 44) | (num == -1)

def puntoValido(lat, lon, parquet, src, transformer):
  noIncendio = not esIncendio(lat, lon, parquet)
  noAguaUrbano = not esAguaUrbano(lat, lon, src, transformer)

  return noIncendio and noAguaUrbano

def filtrarZona(mascarasRegiones, parquetAnio): #Pasamos la lista de parquets de las mascaras y el parquet del año que queremos
  parquetsZonas = []
  for mascaraZona in mascarasRegiones:
      cliente = minioFunctions.crear_cliente()
      zona = minioFunctions.bajar_fichero(cliente, mascaraZona, "gdf")
      gdf = gpd.GeoDataFrame(
          parquetAnio,
          geometry=gpd.points_from_xy(parquetAnio.lon_mean, parquetAnio.lat_mean),
          crs="EPSG:4326"
      )
      zona = zona.to_crs(gdf.crs)
      gdf = gdf.to_crs(zona.crs) #Transforma al sistema de coordenadas de la zona
      mascara = gdf.geometry.within(zona.geometry.iloc[0], align=False) #Crea el filtro de los puntos que pertenecen a la zona estudiada
      gdf_filtrado = gdf[mascara].copy()
      parquetsZonas.append(gdf_filtrado.drop(columns="geometry")) #Devuelve el parquet de esa zona
  return parquetsZonas

def crearFecha(dia, mes, anio):
  fecha = date(anio, mes, dia)
  return fecha.strftime("%Y-%m-%d")