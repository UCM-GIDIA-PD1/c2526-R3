import requests
import numpy as np

def altura(lat, lon):
  ''' Esta función devuelve la altura del punto pasado como parámetro utilizando la api
  de open elevation '''
  url = f"https://api.open-elevation.com/api/v1/lookup?locations={lat},{lon}"
  response = requests.get(url).json()
  return response['results'][0]['elevation']

def pendiente(lat, lon):
  ''' Esta función calcula la pendiente de un punto utilizando el algoritmo de Horn utilizando puntos
  a una distancia de 30 metros '''

  # Vector de direcciones para poder hacer el recorrido matricial en un bucle
  vector_dir = np.array([(-1,-1), (-1,0), (-1, 1),
                         (0, -1), (0, 0), (0, 1),
                         (1, -1), (1, 0), (1, 1)])

  metros = 30

  # Para pasar de metros a grados tanto en la lat como la lon
  delta_lat = metros / 111111.0
  delta_lon = metros / (111111.0 * np.cos(np.radians(lat)))
  puntos_consulta = []

  for dy, dx in vector_dir:
      p_lat = lat + (dy * delta_lat)
      p_lon = lon + (dx * delta_lon)
      puntos_consulta.append((p_lat, p_lon))

  # Obtiene las alturas de los 9 puntos necesarios para calcular la pendiente
  h = [altura(p[0], p[1]) for p in puntos_consulta]

  no, n, ne = h[0], h[1], h[2]
  o,  c,  e  = h[3], h[4], h[5]
  so, s,  se = h[6], h[7], h[8]

 # Algoritmo de Horn para calcular las diferencias en elevación en las dos direcciones
  dz_dx = ((ne + 2*e + se) - (no + 2*o + so)) / (8 * metros)
  dz_dy = ((no + 2*n + ne) - (so + 2*s + se)) / (8 * metros)

  # Calculo de la pendiente tanto en grados como en % de elevación
  pendiente = np.sqrt(np.square(dz_dx) + np.square(dz_dy))
  pend_grados = np.degrees(np.arctan(pendiente))
  pend_porcentaje = pendiente*100

  return {
      "elevacion_centro": c,
      "grados" : pend_grados,
      "porcentaje" : pend_porcentaje
      }
