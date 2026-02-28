import numpy as np
import ee
import asyncio
import time
from . import incendios, minioFunctions
import pandas as pd

sem_global = asyncio.Semaphore(10)

async def pendiente(lat, lon, date, indice = None): #Ignacio: añadido date
  """
    Calcula la elevacion y pendiente (en grados y porcentaje) de un punto usando Google Earth Engine.
    
    Importante:
    - Utiliza el dataset MERIT/DEM/v1_0_3.
    - Asume que Earth Engine siempre devolvera un diccionario con las claves 'dem' y 'slope'.
    - Si el valor de 'slope' es vacio o 0, el calculo del porcentaje asume 0 por defecto.
    """
  async with sem_global:
    elev = ee.Image('MERIT/DEM/v1_0_3').select('dem')
    punto = ee.Geometry.Point([lon, lat])

    slope = ee.Terrain.slope(elev)

    data = elev.addBands(slope).reduceRegion(
        reducer=ee.Reducer.mean(),
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
        "elevacion_centro": res.get('dem'),
        "grados": res.get('slope'),
        "porcentaje": (np.tan(np.radians(res['slope'])) * 100) if res['slope'] else 0
    }

async def df_pendiente(fires, limit = 20, fecha_ini = None, fecha_fin = None):
  
    """
    Extrae la informacion del terreno de una serie de incendios 
    
    Requiere que el DataFrame fires contenga las columnas 'lat_mean', 'lon_mean' y 'date_first'.
    """

    ini = time.time()

    print("Comenzando extracción...")

    #fires = incendios.fetch_fires(filepath, limit, fecha_ini, fecha_fin)
    
    
    #cliente = minioFunctions.crear_cliente()
    #fires = minioFunctions.bajar_fichero(cliente, filepath, "df")

    if limit == -1:
        rows = fires.to_dict('records')
    else:
        rows = fires.head(limit).to_dict('records')

    tareas = [
        pendiente(
            row['lat_mean'],
            row['lon_mean'],
            row['date_first'],
            indice=i
        )
        for i, row in enumerate(rows)
    ]
    resultados = await asyncio.gather(*tareas)
    final_df = pd.DataFrame(resultados)

    fin = time.time()

    print(f"Extraidas {len(final_df)} filas de pendiente en {fin - ini:.2f} segundos.")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Pendiente/")

    return final_df
  

