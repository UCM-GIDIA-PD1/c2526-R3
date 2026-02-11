import rasterio
import pandas as pd
from pyproj import Transformer

def obtenerNumero(lat, lon):
  with rasterio.open("/content/mapa.tif") as src:
      transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
      x, y = transformer.transform(lon, lat)
      row, col = src.index(x, y)
      num = src.read(1)[row, col]
      return num - 1

def entorno(lat, lon):
  num = obtenerNumero(lat, lon)
  df = pd.read_excel("/content/mapa_vegetacion.xlsx")
  #return df.iloc[num]["Column6"] --> Para el ejemplo
  return {"entorno" : df.iloc[num]["Column6"]}