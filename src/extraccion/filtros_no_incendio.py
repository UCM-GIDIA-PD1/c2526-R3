from extraccion.descartadas import vegetacion2
from . import minioFunctions
import numpy as np
import pandas as pd
import geopandas as gpd 
from datetime import date, timedelta
import random

# Filtros encargados de mejorar la precisión y utilidad de datos de no incendio generados

def esIncendio(lat, lon, incendios):
    '''
    Función para determinar si este punto ya se encuentra registrado en el dataset como incendio

    :param lat: Latitud
    :param lon: Longitud
    :param incendios: DataFrame con los datos de incendios registrados
    :return bool: Booleano indicando si es incendio
    '''
  
    esIncendio = ((incendios.lat == lat) & (incendios.lon == lon)).any() 
    return esIncendio


def esAguaUrbano(lat, lon, src, transformer):
    '''
    Función para determinar si este punto se encuentra en el agua o en zona urbana. 
    (Esta función solo sirve para las zonas registradas en Europa)

    :param lat: Latitud
    :param lon: Longitud
    :param src: Archivo raster del .tif de vegetación
    :param transformer: Transformador de coordenadas geográficas
    :return bool: Booleano indicando si está en agua o zona urbana
    '''

    aguaUrbano = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 34, 33, 36, 37, 38, 39, 40, 41, 42, 43]
    num = vegetacion2.obtenerNumero(lat, lon, src, transformer)
    return (np.isin(num, aguaUrbano)) | (num == 44) | (num == -1) #-1 o 44 significa noData


def puntoValido(lat, lon, parquet, src, transformer):
    '''
    Función para determinar si este punto cumple las condiciones de las funciones anteriores

    :param lat: Latitud
    :param lon: Longitud
    :param parquet: DataFrame con los datos de incendios
    :param src: Archivo raster del .tif de vegetación
    :param transformer: Transformador de coordenadas geográficas
    :return bool: Booleano indicando si el punto es válido
    '''
  
    noIncendio = not esIncendio(lat, lon, parquet)
    #noAguaUrbano = not esAguaUrbano(lat, lon, src, transformer)

    #return noIncendio and noAguaUrbano
    return noIncendio


def filtrarZona(mascarasRegiones, parquetAnio, cliente, devolver_lista = True): #Pasamos la lista de parquets de las mascaras y el parquet del año que queremos
    '''
    Función para filtrar los puntos, según la zona biogeográfica a la que pertenezcan

    :param mascarasRegiones: Lista con las rutas de todas las máscaras de las regiones
    :param parquetAnio: Parquet con los datos de incendio de un año determinado
    :param cliente: Cliente MinIO
    :param devolver_lista: Parámetro de personalización de la salida, se puede devolver una lista o el dataframe completo
    :return list: Lista con los DataFrames divididos por zona biogeográfica
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
    
    if devolver_lista: return parquetsZonas
    else: 
        parquetCompleto = pd.concat(parquetsZonas, ignore_index=True).drop_duplicates()
        return parquetCompleto #Devuelve el parquet completo con todas las zonas filtradas, sin la columna de geometría


def filtrar_zona_eliminar(ruta, df, cliente):
    '''
    Función para filtrar los puntos pertenecientes a una zona determinada

    :param ruta: Ruta de la máscara de la zona
    :param df: DataFrame con todos los puntos
    :param cliente: Cliente MinIO
    :return pd.DataFrame: DataFrame con los puntos pertenecientes a esa zona
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

    :param dia: Día
    :param mes: Mes
    :param anio: Año
    :return str: Fecha en formato string (YYYY-MM-DD)
    '''
    hoy = date.today()
    
    try:
        fecha_solicitada = date(anio, mes, dia)

    except Exception:
        
        if mes == 2 and dia >= 29:
            fecha_solicitada = date(anio, 2, 28)
        else:
            fecha_solicitada = date(anio, mes, 1)

    if fecha_solicitada > hoy:

        # Para el 1 de enero
        if hoy.month == 1 and hoy.day == 1:
            fecha_final = hoy
        else:
            nuevo_mes = random.randint(1, hoy.month)
            
            # Controlamos los valores de día 31 
            if nuevo_mes < hoy.month:
                if nuevo_mes == 12:
                    ultimo_dia = 31
                else:
                    ultimo_dia = (date(anio, nuevo_mes + 1, 1) - timedelta(days=1)).day

                nuevo_dia = random.randint(1, ultimo_dia)
            else:
                nuevo_dia = random.randint(1, hoy.day)
            
            fecha_final = date(anio, nuevo_mes, nuevo_dia)
    else:
        fecha_final = fecha_solicitada
    
    return fecha_final.strftime("%Y-%m-%d")