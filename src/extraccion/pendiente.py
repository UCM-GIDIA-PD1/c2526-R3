import numpy as np
import ee
import asyncio
import time
from . import incendios
import pandas as pd

async def pendiente(lat, lon, indice = None):
  ''' Esta función calcula la pendiente de un punto utilizando el algoritmo de Horn utilizando puntos
  a una distancia de 30 metros '''

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
      "elevacion_centro": res['elevation'],
      "grados": res['slope'],
      "porcentaje": (np.tan(np.radians(res['slope'])) * 100) if res['slope'] else 0
  }

async def df_pendiente(filepath, limit = 20):
  ini = time.time()
  print("Comenzando extracción...")
  fires = incendios.fetch_fires(filepath, limit)
  tareas = [
        pendiente(row['lat_mean'], row['lon_mean'], indice = i)
        for i, row in enumerate(fires.head(limit).to_dict('records'))
    ]
  resultados = await asyncio.gather(*tareas)
  final_df = pd.DataFrame(resultados)
  fin = time.time()
  print(f"Extraidas {limit} filas de pendiente en {fin - ini:.2f} segundos.")
  print(final_df.head(limit))
  return final_df
  