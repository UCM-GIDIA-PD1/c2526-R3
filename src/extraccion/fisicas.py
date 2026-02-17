import requests

def fetch_environment(lat, lon, date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    date = date.strftime("%Y-%m-%d")
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "daily": ",".join([
            "temperature_2m_mean",
            "temperature_2m_max",
            "temperature_2m_min",
            "relative_humidity_2m_mean",
            "precipitation_sum",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "surface_pressure_mean",
            "cloud_cover_mean",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration",
            "sunshine_duration"
        ]),
        "timezone": "UTC"
    }

    r = requests.get(url, params=params).json()
    d = r["daily"]

    return {
        "temp_mean": d["temperature_2m_mean"][0],
        "temp_max": d["temperature_2m_max"][0],
        "temp_min": d["temperature_2m_min"][0],
        "humidity_mean": d["relative_humidity_2m_mean"][0],
        "precipitation": d["precipitation_sum"][0],
        "wind_speed_max": d["wind_speed_10m_max"][0],
        "wind_gusts_max": d["wind_gusts_10m_max"][0],
        "pressure_mean": d["surface_pressure_mean"][0],
        "cloud_cover": d["cloud_cover_mean"][0],
        "radiation": d["shortwave_radiation_sum"][0],
        "evapotranspiration": d["et0_fao_evapotranspiration"][0],
        "sunshine_seconds": d["sunshine_duration"][0]
    }
