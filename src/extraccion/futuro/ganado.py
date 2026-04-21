'''
Información de la densidad de ganado a partir de la info del Gridded Livestock Density de la FAO.
Fuente: https://data.apps.fao.org/catalog//iso/9d1e149b-d63f-4213-978b-317a8eb42d02
'''

import rasterio
import pandas as pd
from pyproj import Transformer

import time
from rasterio.windows import Window
from dotenv import load_dotenv

import asyncio
from extraccion import minioFunctions
import numpy as np

def obtenerNumero(lat, lon,matriz, src, transformer):

    '''
    Extrae el valor del pixel correspondiente a una coordenada dentro de un archivo raster
    
    Casos limite:
    - Si no hay nada que leer devuelve -1
    - Cualquier valor negativo se reasigna al indice 44

    El código comentado corresponde a la búsqueda de puntos cercanos para sanear puntos nulos.
    Como esta función también es usada para crear puntos sintéticos, queda comentado
    
    :param lat: Latitud
    :param lon: Longitud
    :param matriz: Matriz de datos
    :param src: Archivo raster
    :param transformer: Transformador de coordenadas
    :return float: Valor del pixel
    '''
     
    x, y = transformer.transform(lon, lat)
    row, col = src.index(x, y)

    num = matriz[row, col]

    if num == src.nodata or num < 0:
        
        if num < 0:
            #Window de 3x3 y restamos a col y row 2 para posicionarnos en medio  
            # Hay que tener cuidado con no salir de los límites del raster   
            row_min = max(0, row - 2)
            row_max = min(src.height, row + 3)
            col_min = max(0, col - 2)
            col_max = min(src.width, col + 3)
            
            # CORTAMOS LA MATRIZ DE RAM (Cero peticiones de red)
            data_vecinos = matriz[row_min:row_max, col_min:col_max]

            if data_vecinos.size > 0:
                vecinos_clean = np.where((data_vecinos == src.nodata) | (data_vecinos < 0), np.nan, data_vecinos)
            
                # Comprobamos si hay al menos un vecino válido
                if not np.isnan(vecinos_clean).all():
                    media_vecinos = np.nanmean(vecinos_clean)
                    return float(media_vecinos)
        
    else:
        data_vecinos = matriz[row_min:row_max, col_min:col_max]
      
    return num

def lista_entorno(lista_puntos): 

    """
    Mapea una lista de coordenadas a su densidad de cada tipo de ganado
    
    Abre una conexion al raster alojado en MinIO y traduce el valor numerico de cada 
    pixel utilizando el indice de df_vegetacion
    
    :params lista_puntos: Lista de tuplas
    :return dict: Diccionario con las densidades
    """

    load_dotenv()
    
    minio_config = {
        "AWS_S3_ENDPOINT": "minio.fdi.ucm.es",
        "AWS_HTTPS": "YES",
        "AWS_VIRTUAL_HOSTING": "FALSE",
        "GDAL_HTTP_UNSAFESSL": "YES",
    }
    ak, sk = minioFunctions.importar_keys()

    with rasterio.Env(**minio_config,
                     aws_access_key_id=ak,
                     aws_secret_access_key=sk):

        animales = ['bufalos','cabras','cerdos','chicken','ovejas','vacuno']
        densidades = {}
        
        for animal in animales:

            with rasterio.open(f"/vsis3/pd1/grupo3/maps/ganado/densidad_{animal}.tif") as src:
                print(f"Número total de bandas: {src.count}")
                print(f"Índices de las bandas: {src.indexes}")
                print(f"Limites del mapa .tif: {src.bounds}")
                print(f"Sistema de coordenadas usado: {src.crs}")

                transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
                lista_densidad = []

                matriz = src.read(1)
            
                for i, (lon, lat) in enumerate(lista_puntos):
                    num = obtenerNumero(lat, lon, matriz, src, transformer)
                    lista_densidad.append(num)
                
                    if i % 1000 == 0:
                        print(f"Dato {i} extraído de {animal}")
                densidades[animal] = lista_densidad

        return densidades
    
async def df_ganado(fires, limit=20, fecha_ini=None, fecha_fin=None):

    """
    Se extraen los datos de vegetacion para un dataset
    
    Requiere que el DataFrame fires contenga las columnas 'lat_mean', 'lon_mean' y 'date_first'
    
    :params fires: Dataframe con los puntos
    :params limit: Límite de filas
    :params fecha_ini: Fecha de inicio
    :params fecha_fin: Fecha de fin
    :return pd.DataFrame: DataFrame final
    """
    
    ini = time.time()
    ak, sk = minioFunctions.importar_keys()

    fin_none = fecha_fin is None
    ini_none = fecha_ini is None

    if not fin_none and not ini_none: 
        fires = fires[fires['date_first'].between(fecha_ini, fecha_fin)]

    if limit != -1:
        fires = fires.head(limit)   
    
    lista_puntos = list(zip(fires['lon_mean'], fires['lat_mean']))

    try:
        lista_res = await asyncio.to_thread(lista_entorno, lista_puntos)
    except KeyboardInterrupt:
        print("\n Interrupción detectada. No hay datos parciales para guardar en ganado (proceso síncrono).")
        raise
    
    fires = fires[['lat_mean','lon_mean','date_first']].copy().reset_index(drop = True)

    final_df = pd.DataFrame(lista_res)
    final_df = pd.concat([fires, final_df], axis = 1)
    final_df = final_df.rename(columns={'lat_mean':'lat', 'lon_mean':'lon', 'date_first':'date'})

    print(f"Finalizado en {time.time() - ini:.2f}s")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/ganado/")

    return final_df