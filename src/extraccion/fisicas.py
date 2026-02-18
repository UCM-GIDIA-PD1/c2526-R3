import asyncio

async def fetch_environment(session, lat, lon, date, intentos=3):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date,
        "end_date": date,
        "daily": [
            "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
            "relative_humidity_2m_mean", "precipitation_sum", "wind_speed_10m_max",
            "wind_gusts_10m_max", "surface_pressure_mean", "cloud_cover_mean",
            "shortwave_radiation_sum", "et0_fao_evapotranspiration", "sunshine_duration"
        ],
        "timezone": "UTC"
    }

    for i in range(intentos):
        try:
            async with session.get(url, params=params) as response:
                r = await response.json()

                if "daily" in r:
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
                else:
                
                    error_msg = r.get('reason') or r.get('error') or "Error desconocido"
                    print(f"Intento {i+1} fallido en ({lat}, {lon}): {error_msg}")
                    await asyncio.sleep(1 * (i + 1))

        except Exception as e:
            print(f"Error de conexión: {e}")
            await asyncio.sleep(1)

    return {k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                              "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                              "radiation", "evapotranspiration", "sunshine_seconds"]}