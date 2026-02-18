import numpy as np
import ee
import asyncio

async def pendiente(lat, lon):
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

  return {
      "elevacion_centro": res['elevation'],
      "grados": res['slope'],
      "porcentaje": (np.tan(np.radians(res['slope'])) * 100) if res['slope'] else 0
  }
