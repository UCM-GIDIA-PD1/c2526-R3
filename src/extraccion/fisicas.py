import asyncio
import time
from . import incendios
import pandas as pd
import aiohttp

sem_global = asyncio.Semaphore(20)

async def fetch_environment(session, lat, lon, date, indice = None, intentos=3):
    async with sem_global:
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
                        print(f"Características físicas {indice} extraidas.")
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
                        print(f"Intento {i+1} fallido en ({indice}, {lat:.2f}, {lon:.2f}): {error_msg}")
                        await asyncio.sleep(1 * (i + 1))

            except Exception as e:
                print(f"Error de conexión: {e}")
                await asyncio.sleep(1)

        return {k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                                "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                                "radiation", "evapotranspiration", "sunshine_seconds"]}

async def df_fisicas(filepath, limit = 20):
    
    async with aiohttp.ClientSession() as session:
        ini = time.time()

        print("Comenzando extracción...")

        fires = incendios.fetch_fires(filepath, limit)
        tareas = [
            fetch_environment(session = session, lat = row['lat_mean'], lon = row['lon_mean'], date = row['date_first'].strftime('%Y-%m-%d'), indice = i,)
            for i, row in enumerate(fires.head(limit).to_dict('records'))
        ]
        resultados = await asyncio.gather(*tareas)
        final_df = pd.DataFrame(resultados)

        fin = time.time()

        print(f"Extraidas {limit} filas de características físicas en {fin - ini:.2f} segundos.")
        print(final_df.head(limit))
        return final_df
