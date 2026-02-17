import requests
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

def limpieza(df):
  assert not df.empty, "No se pueden analizar incendios, el DataFrame esta vacio"
  df['acq_date'] = pd.to_datetime(df['acq_date'])
  df = df[df['confidence'] != 'l'] #confianza l (low) es que no es fiable
  df = df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date']) #quita incendios iguales
  df= df[df['frp'] > 50]
  df = df.rename(columns={
        'latitude': 'lat',
        'longitude': 'lon',
        'acq_date': 'date'
  })
  columnas_utiles = ['lat', 'lon', 'frp', 'date']
  return df[columnas_utiles]


def separate_fire_events(df, dist_km=2.0):
    """
    Asigna un ID único a cada grupo de puntos que pertenezcan al mismo incendio.

    Parámetros:
    - df: DataFrame de FIRMS.
    - dist_km: Distancia máxima para considerar que dos puntos son del mismo incendio.
    """
    assert not df.empty, "El DataFrame contenia fuegos poco relevantes y se vacio, no se pueden separar eventos de incendios"

    # 1. Convertir coordenadas a radianes para usar con la métrica haversine
    coords = np.radians(df[['lat', 'lon']])

    # 2. Configurar el algoritmo
    # El radio de la Tierra es ~6371 km. eps = distancia / radio_tierra
    kms_per_radian = 6371.0
    epsilon = dist_km / kms_per_radian

    db = DBSCAN(eps=epsilon, min_samples=1, metric='haversine').fit(coords)

    # 3. Asignar las etiquetas al DataFrame
    df['fire_id'] = db.labels_

    # Opcional: Contar cuántos puntos hay por incendio y su FRP total
    resumen = df.groupby('fire_id').agg({
        'lat': 'mean',
        'lon': 'mean',
        'frp': ['sum', 'mean', 'count'],
        'date': ['first', 'last']
    })

    df['lat'] = df['lat'].round(2)
    df['lon'] = df['lon'].round(2)

    resumen.columns = ['lat_mean', 'lon_mean', 'frp_sum', 'frp_mean', 'count', 'date_first', 'date_last']

    resumen['duration_days'] = (resumen['date_last'] - resumen['date_first']).dt.days + 1
    return df, resumen

def fetch_fires(filepath, round_decimals=2):
    """
    la funcion devuelve un dataFrame:
    resumen es lo mas importante y contiene un dataFrame con:
    -   LAT_MEAN: la media de las latitudes de los puntos que pertenecen al mismo incendio
    -   LON_MEAN: la media de las longitudes de los puntos que pertenecen al mismo incendio
    -   FRP - sum, mean: la suma y media de los FRP de los puntos que pertenecen al mismo incendio
    -   COUNT indica la cantidad de puntos que pertenecen al mismo incendio
    -   DATE_FIRST: la fecha del primer punto del incendio
    -   DATE_LAST: la fecha del ultimo punto del incendio
    """

    df = pd.read_csv(filepath)
    df_clean = limpieza(df)
    df_clean, resumen = separate_fire_events(df_clean, 5.0)

    resumen['lat_mean'] = resumen['lat_mean'].round(round_decimals)
    resumen['lon_mean'] = resumen['lon_mean'].round(round_decimals)
    resumen['title'] = 'Fire at ' + resumen['lat_mean'].astype(str) + ', ' + resumen['lon_mean'].astype(str) + ' on ' + resumen['date_first'].dt.strftime('%Y-%m-%d')


    return resumen.sort_values(by='count', ascending=False)



'''
def fetch_fires(fecha_ini, fecha_end):
    #fecha_ini y fecha_end es tipo string. El formato debe ser "YYYY-MM-DD"

    url = "https://eonet.gsfc.nasa.gov/api/v3/events?start="+fecha_ini+"&end="+ fecha_end
    params = {
        "status": "open",
        "category": "wildfires"
    }

    data = requests.get(url, params=params).json()
    rows = []

    for event in data["events"]:
        eid = event["id"]
        title = event["title"]

        for geom in event["geometry"]:
            date = geom["date"][:10]
            lon, lat = geom["coordinates"]
            rows.append([eid, title, date, lat, lon])

    return pd.DataFrame(
        rows,
        columns=["id", "title", "date", "lat", "lon"]
    )

'''
