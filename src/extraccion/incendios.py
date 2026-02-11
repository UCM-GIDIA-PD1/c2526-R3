import requests
import pandas as pd

def fetch_fires():
    url = "https://eonet.gsfc.nasa.gov/api/v3/events"
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
