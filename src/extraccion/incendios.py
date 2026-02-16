import requests
import pandas as pd

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

def fetch_fires(filepath, round_decimals=2):
    """
    
    Carga un archivo CSV de FIRMS y devuelve un DataFrame con columnas: id, title, date, lat, lon
    
    """

    df = pd.read_csv(filepath)

    df = df.rename(columns={
        'latitude': 'lat',
        'longitude': 'lon',
        'acq_date': 'date'
    })

    df['lat_round'] = df['lat'].round(round_decimals)
    df['lon_round'] = df['lon'].round(round_decimals)

  
    df_clean = df.groupby(['date', 'lat_round', 'lon_round'], as_index=False).first()
    
    df_clean = df_clean.drop(columns=['lat_round', 'lon_round'])

    df_clean['id'] = df_clean.index.astype(str)
    df_clean['title'] = 'Fire at ' + df_clean['lat'].astype(str) + ', ' + df_clean['lon'].astype(str) + ' on ' + df_clean['date']

    return df_clean[['id', 'title', 'date', 'lat', 'lon']]



