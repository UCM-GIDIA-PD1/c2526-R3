import requests
import numpy as np

def altura(lat, lon):
  url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
  response = requests.get(url).json()
  return response['results'][0]['elevation']

def pendiente(lat, lon):
  vector_dir = np.array([(-1,-1), (-1,0), (-1, 1),
                         (0, -1), (0, 0), (0, 1),
                         (1, -1), (1, 0), (1, 1)])

  metros = 30
  delta_lat = metros / 111111.0
  delta_lon = metros / (111111.0 * np.cos(np.radians(lat)))
  puntos_consulta = []
  for dy, dx in vector_dir:
      p_lat = lat + (dy * delta_lat)
      p_lon = lon + (dx * delta_lon)
      puntos_consulta.append((p_lat, p_lon))

  h = [altura(p[0], p[1]) for p in puntos_consulta]

  no, n, ne = h[0], h[1], h[2]
  o,  c,  e  = h[3], h[4], h[5]
  so, s,  se = h[6], h[7], h[8]

  dz_dx = ((ne + 2*e + se) - (no + 2*o + so)) / (8 * metros)
  dz_dy = ((no + 2*n + ne) - (so + 2*s + se)) / (8 * metros)

  pendiente = np.sqrt(np.square(dz_dx) + np.square(dz_dy))
  pend_grados = np.degrees(np.arctan(pendiente))
  pend_porcentaje = pendiente*100

  return {
      "elevacion_centro": c,
      "grados" : pend_grados,
      "porcentaje" : pend_porcentaje
      }
