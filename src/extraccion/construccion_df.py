from . import incendios, pendiente, vegetacion, fisicas, minioFunctions, puntos_sinteticos
from limpieza import limpieza
from analisis import analisis
import time
import pandas as pd
import asyncio
import aiohttp
import os
import numpy as np

sem_global = asyncio.Semaphore(20)

async def procesar_fila_completa(session, row, index, directo):

    """
    Extrae las caracteristicas ambientales para una sola observacion.
    
    Importante:
    - Asume que 'row' es una tupla que contiene los atributos 'date_first', 'lat_mean' y 'lon_mean'.
    - Aplica un retraso escalonado (index * 0.1) para disminuir el riesgo de bloqueos por parte de las APIs
    """

    async with sem_global:
        
        await asyncio.sleep(index * 0.1)
        fecha_str = row.date_first.strftime('%Y-%m-%d')
        tareas = [
            fisicas.fetch_environment(session, row.lat_mean, row.lon_mean, fecha_str, directo),
            vegetacion.vegetacion(row.lat_mean, row.lon_mean, fecha_str),
            pendiente.pendiente(row.lat_mean, row.lon_mean, fecha_str),
        ]

        resultados = await asyncio.gather(*tareas)

        env_datos = {**resultados[0], **resultados[1], **resultados[2]}
        print("Fila extraida")
        return env_datos
    
async def build_environmental_df(file, limit=100, fecha_ini=None, fecha_fin=None, directo = False):
    
    """
    Construye el DataFrame uniendo informacion de incendios con variables fisicas, topograficas y de vegetacion
    
    Importante:
    - Se asume que el indice generado por fetch_fires coincide secuencialmente 
      con el orden procesado lo que permite la concatenacion lateral (axis=1) directa
    """

    ini = time.time()

    assert isinstance(file, pd.DataFrame), f"No es un DataFrame"

    fires = incendios.fetch_fires(file, fecha_ini, fecha_fin)
    
    assert not fires.empty, "El DataFrame está vacio"

    no_fires = puntos_sinteticos.crearSinteticos(fires, False)
    
    no_fires = no_fires.rename(columns={'lat': 'lat_mean', 'lon': 'lon_mean', 'date': 'date_first'})
    
    for col in fires.columns:
        if col not in no_fires.columns and col != 'final':
            no_fires[col] = pd.NA

    fires["final"] = 1
    no_fires["final"] = 0

    merged = pd.concat([fires, no_fires], ignore_index=True)
    merged['date_first'] = pd.to_datetime(merged['date_first'])

    async with aiohttp.ClientSession() as session:

        tareas_totales = [
            procesar_fila_completa(session, row, i, directo)
            for i, row in enumerate(merged.head(limit).itertuples())
        ]

        print(f"Iniciando extracción: {limit} puntos...")
        env_rows = await asyncio.gather(*tareas_totales)
        env_df = pd.DataFrame(env_rows)

    merged = merged.head(limit)
    merged = merged.reset_index(drop=True)
    env_df = env_df.reset_index(drop=True)
    env_df = pd.concat([merged, env_df], axis=1)
    final_df = limpieza.limpieza(env_df)

    fin = time.time()
    print(f"Extraidos {limit} puntos en {fin - ini:.2f} segundos.")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Incendios_environmental/")

    return final_df
    
#Ignacio: lo mejor es pasar como primer elemento de la lista el parquet de los
#incendios/no incendios con los puntos para que el merge(how = 'left') sea más robusto
def merge_parquets(path_list, anio):
    """
    Realiza un 'outer join' iterativo sobre una lista de DataFrames. 
    
    Condiciones:
    - El primer dataframe debe ser el de incendios y no incendios

    :param path_list: lista con los paths de las variables a juntar
    :param anio: año de las variables
    :return incendios_y_no_incendios: dataframe final con todas las variables
    """

    assert len(path_list) >= 2, "Longitud de la lista pasada por parámetro no válida."
    
    #Trabajo con MinIO
    cliente = minioFunctions.crear_cliente()
    incendios_y_no_incendios = minioFunctions.bajar_fichero(cliente, path_list[0], "df")
    incendios_y_no_incendios.rename(
        columns={
            "date_first" : "date",
            "lat_mean" : "lat",
            "lon_mean" : "lon"
        }, inplace = True
    )

    #Las seleccionamos para posteriormente tratar los nulos
    columns = incendios_y_no_incendios.columns
    columns = [col for col in columns if col not in ['date', 'date_last', 'date_first']]

    #Saneamos el problema con las fechas (distintos formatos en los distintos dataframes)
    incendios_y_no_incendios['date'] = pd.to_datetime(incendios_y_no_incendios['date'], format='mixed').dt.normalize()

    #Vamos haciendo merge
    for path in path_list[1:]:
        df = minioFunctions.bajar_fichero(cliente, path, "df")
        df['date'] = pd.to_datetime(df['date'], format='mixed').dt.normalize()
        incendios_y_no_incendios = pd.merge(incendios_y_no_incendios, df, on=["lat", "lon", "date"], how='outer')
    
    #Tratamos nulos en no incendios (estableciéndolos a 0)
    incendios_y_no_incendios[columns] = incendios_y_no_incendios[columns].fillna(0)

    #Subimos a MinIO
    print(f"Dataframe final:\n {incendios_y_no_incendios.head(5)}")
    minioFunctions.subir_fichero(cliente, f"grupo3/raw/Final/final_{anio}.parquet", incendios_y_no_incendios)
    print(f"Merge hecho correctamente sobre el año {anio}")
    
    return incendios_y_no_incendios

def juntar_incendios():
    
    """
    Descarga, etiqueta y concatena los puntos historicos de incendios y no incendios.
    
    Suposiciones:
    - Se supone que los archivos de incendios y no incendios estan organizados en carpetas separadas 
      y que cada archivo de una carpeta tiene un archivo correspondiente en la otra carpeta con el mismo año cronologico. 
    - Define la variable objetivo 'final', asignando 1 a eventos de incendio y 0 a puntos de no incendio para clasificar
    """

    #Definir paths
    path1 = "grupo3/raw/incendios"
    path2 = "grupo3/raw/No_incendios"

    #Localizar carpetas
    cliente = minioFunctions.crear_cliente()
    incendios = cliente.list_objects("pd1", prefix = path1 , recursive = True)
    no_incendios = cliente.list_objects("pd1", prefix = path2 , recursive = True)

    #Iterar por años
    for incendio, no_incendio in zip(incendios, no_incendios):
        df_inc = minioFunctions.bajar_fichero(cliente, incendio.object_name, "df")
        df_no_inc = minioFunctions.bajar_fichero(cliente, no_incendio.object_name, "df")
        print(f"Bajados correctamente {incendio.object_name} y {no_incendio.object_name}")

        #Clasificación binaria
        df_inc["final"] = 1
        df_no_inc["final"] = 0
        
        #Outer join sobre las columnas de no_incendios => las columnas extra de "incendios" en "no_incendios" seran NaN
        merged = pd.concat([df_inc, df_no_inc], ignore_index=True)

        merged['date_first'] = merged['date_first'].astype(str)
        
        #Subimos a minio
        anio = incendio.object_name.split("_")[-1] #Cogemos el año y extensión .parquet
        path_destino = f"grupo3/raw/Incendios_y_no_incendios/incendios_y_no_incendios_{anio}"
        minioFunctions.subir_fichero(cliente, path_destino, merged)
        print(f"Subidos a: {path_destino}")

def concatenar_df():
    variable = input("Que variable quieres concatenar: ")

    anyo = input(f'De que año quieres concatenar los archivos para {variable}? (2022-2025) ')
    cliente = minioFunctions.crear_cliente()
    carpeta_fisicas = f"grupo3/raw/{variable}"
    elementos = cliente.list_objects('pd1', prefix = carpeta_fisicas, recursive = True)

    archs_anyo = [elem.object_name for elem in elementos if elem.object_name.endswith(f'{anyo}.parquet')]
    dfs = []

    for arch in archs_anyo:
        dfs.append(minioFunctions.bajar_fichero(cliente, arch))
    
    df = pd.concat(dfs)

    minioFunctions.preguntar_subida(df, f"grupo3/raw/{variable}/")