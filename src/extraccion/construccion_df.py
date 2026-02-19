from . import incendios, pendiente, vegetacion, fisicas, vegetacion2
import time
import pandas as pd
import asyncio
import aiohttp
import os

sem_global = asyncio.Semaphore(20)

async def procesar_fila_completa(session, row, index):
    async with sem_global:
        
        await asyncio.sleep(index * 0.1)
        fecha_str = row.date_first.strftime('%Y-%m-%d')
        tareas = [
            fisicas.fetch_environment(session, row.lat_mean, row.lon_mean, fecha_str),
            vegetacion.vegetacion(row.lat_mean, row.lon_mean, fecha_str),
            pendiente.pendiente(row.lat_mean, row.lon_mean),
        ]

        resultados = await asyncio.gather(*tareas)

        env_datos = {**resultados[0], **resultados[1], **resultados[2]}
        print("Fila extraida")
        return env_datos
    
async def build_environmental_df(filepath, limit=100, fecha_ini = None, fecha_fin = None):
    
    ini = time.time()

    async with aiohttp.ClientSession() as session:
        fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)

        tareas_totales = [
            procesar_fila_completa(session, row, i)
            for i, row in enumerate(fires.head(limit).itertuples())
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
    fires = fires.head(limit)
    lista_puntos = list(zip(fires['lon_mean'], fires['lat_mean']))
    veg2_resultados = await asyncio.to_thread(vegetacion2.lista_entorno, lista_puntos, df_aux)
    env_df = pd.concat([fires, env_df], axis = 1)
    final_df = pd.concat([fires.reset_index(drop=True), env_df], axis=1)

    fin = time.time()
    print(f"Extraidos {limit} incendios en {fin - ini:.2f} segundos.")
    print(final_df.head(limit))
    return final_df
    