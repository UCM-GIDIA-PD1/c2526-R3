from . import incendios, pendiente, vegetacion, fisicas, vegetacion2, minioFunctions, puntos_sinteticos
import time
import pandas as pd
import asyncio
import aiohttp
import os

sem_global = asyncio.Semaphore(20)

async def procesar_fila_completa(session, row, index):

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
            fisicas.fetch_environment(session, row.lat_mean, row.lon_mean, fecha_str),
            vegetacion.vegetacion(row.lat_mean, row.lon_mean, fecha_str),
            #Ignacio: pasamos fecha_str
            pendiente.pendiente(row.lat_mean, row.lon_mean, fecha_str),
        ]

        resultados = await asyncio.gather(*tareas)

        env_datos = {**resultados[0], **resultados[1], **resultados[2]}
        print("Fila extraida")
        return env_datos
    
async def build_environmental_df(file, limit=100, fecha_ini = None, fecha_fin = None):
    
    """
    Construye el DataFrame uniendo informacion de incendios con variables fisicas, topograficas y de vegetacion
    
    Importante:
    - Se asume que el indice generado por fetch_fires coincide secuencialmente 
      con el orden procesado lo que permite la concatenacion lateral (axis=1) directa
    """

    ini = time.time()

    async with aiohttp.ClientSession() as session:

        #Enviamos un df directamente
        fires = incendios.fetch_fires(file, limit, fecha_ini, fecha_fin, True)
        no_fires = puntos_sinteticos.crearSinteticos(fires, None)

        fires["final"] = 1
        no_fires["final"] = 0
        
        #Outer join sobre las columnas de no_incendios => las columnas extra de "incendios" en "no_incendios" seran NaN
        merged = pd.concat([fires, no_fires], ignore_index=True)

        merged['date_first'] = merged['date_first'].astype(str)


        tareas_totales = [
            procesar_fila_completa(session, row, i)
            for i, row in enumerate(merged.head(limit).itertuples())
        ]

        print(f"Iniciando extracción: {limit} incendios...")
        env_rows = await asyncio.gather(*tareas_totales)
        env_df = pd.DataFrame(env_rows)

    #Lectura del csv de vegetación 2
    df_aux = pd.read_csv("s3://pd1/grupo3/mapa/mapa_vegetacion.csv", 
                         storage_options={
                             "key": os.getenv("AWS_ACCESS_KEY_ID"),
                             "secret": os.getenv("AWS_SECRET_ACCESS_KEY"),
                             "client_kwargs": {"endpoint_url": "https://minio.fdi.ucm.es", "verify": False}
                         })
    merged = merged.head(limit)
    lista_puntos = list(zip(merged['lon_mean'], merged['lat_mean']))
    veg2_resultados = await asyncio.to_thread(vegetacion2.lista_entorno, lista_puntos, df_aux)
    veg2_resultados = pd.DataFrame(veg2_resultados, columns=["vegetacion2"])

    merged = merged.reset_index(drop=True)
    env_df = env_df.reset_index(drop=True)
    veg2_resultados = veg2_resultados.reset_index(drop=True)
    env_df = pd.concat([merged, env_df], axis=1)
    env_df = pd.concat([veg2_resultados, env_df], axis=1)
    final_df = env_df


    fin = time.time()
    print(f"Extraidos {limit} incendios en {fin - ini:.2f} segundos.")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Incendios_environmental/")
    return final_df
    
#Ignacio: lo mejor es pasar como primer elemento de la lista el parquet de los
#incendios/no incendios con los puntos para que el merge(how = 'left') sea más robusto
def merge_parquets(path_list):
    
    """
    Realiza un 'left join' iterativo sobre una lista de DataFrames alojados en MinIO
    
    Reglas de negocio:
    - Se necesitan como minimo dos rutas de archivo para poder operar
    - Usa 'lat', 'lon' y 'date' como claves de cruce
    """

    assert len(path_list) >= 2, "Longitud de la lista pasada por parámetro no válida."
    cliente = minioFunctions.crear_cliente()
    
    result = minioFunctions.bajar_fichero(cliente, path_list[0], "df")
    for path in path_list[1:]:
        df = minioFunctions.bajar_fichero(cliente, path, "df")
        result['date'] = pd.to_datetime(result['date'])
        df['date'] = pd.to_datetime(df['date'])
        result = pd.merge(result, df, on=["lat", "lon", "date"], how='left')
    
    return result

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