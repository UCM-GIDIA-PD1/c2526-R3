import numpy as np
import ee
import asyncio
import time
from . import incendios, minioFunctions
import pandas as pd

sem_global = asyncio.Semaphore(10)

async def pendiente(lat, lon, date, indice = None): #Ignacio: añadido date
  ''' Esta función calcula la pendiente de un punto utilizando los satelites de Google EE'''
  async with sem_global:
    dem = ee.Image('USGS/SRTMGL1_003')
    punto = ee.Geometry.Point([lon, lat])

    slope = ee.Terrain.slope(dem)

    data = dem.addBands(slope).reduceRegion(
        reducer=ee.Reducer.first(),
        geometry=punto,
        scale=30
    )

    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, lambda: data.getInfo())
    if indice is not None:
      print(f"Pendiente {indice} extraida.")
    
    return {
        #Ignacio: añadido ["lat", "lon", "date"]
        "lat" : lat,
        "lon" : lon, 
        "date" : date,
        "elevacion_centro": res['elevation'],
        "grados": res['slope'],
        "porcentaje": (np.tan(np.radians(res['slope'])) * 100) if res['slope'] else 0
    }

async def df_pendiente(filepath, limit = 20, fecha_ini = None, fecha_fin = None):

  ini = time.time()

  print("Comenzando extracción...")

  #fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)
  
  
  cliente = minioFunctions.crear_cliente()
  fires = minioFunctions.bajar_fichero(cliente, filepath, "df")

  tareas = [
        #Ignacio: pasamos ahora row["date_first"]
        pendiente(row['lat_mean'], row['lon_mean'],row['date_first'], indice = i)
        for i, row in enumerate(fires.head(limit).to_dict('records'))
    ]
  resultados = await asyncio.gather(*tareas)
  final_df = pd.DataFrame(resultados)

  fin = time.time()

  print(f"Extraidas {limit} filas de pendiente en {fin - ini:.2f} segundos.")
  print(final_df.head(limit))

  minioFunctions.preguntar_subida(final_df, "grupo3/raw/Pendiente/")

  return final_df
  

