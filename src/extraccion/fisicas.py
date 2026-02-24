import asyncio
import time
from .  import incendios, minioFunctions
import pandas as pd
import aiohttp

# Funciones encargadas de utilizar la Api de open-meteo para adquirir datos meteorológicos de cada punto

sem_global = asyncio.Semaphore(20)

async def fetch_environment(session, lat, lon, date, indice = None, intentos=3):
    '''
    Consulta la API de Open-Meteo para obtener datos meteorológicos diarios para una ubicación y fecha específicas

    Parámetros:
    - session: instancia de aiohttp.ClientSession para realizar la solicitud HTTP
    - date: fecha para la cual se desean obtener los datos meteorológicos (formato string 'YYYY-MM-DD')
    - indice: índice del punto en el DataFrame (opcional, para fines de depuración)
    - intentos: número de intentos para realizar la solicitud en caso de fallo (por defecto es 3)

    Devuelve:
    - Diccionario con los datos meteorológicos obtenidos o None si no se obtuvieron datos.
    '''
    
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
                        
                        await asyncio.sleep(3)

                        return {
                            "lat" : lat,
                            "lon" : lon,
                            "date" : date,
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

        error = {"lat" : lat, "lon" : lon, "date" : date}
        error.update({k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                                "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                                "radiation", "evapotranspiration", "sunshine_seconds"]})
        return error

async def df_fisicas(fires, limit = 20, fecha_ini = None, fecha_fin = None):
    '''
    Obtiene características físicas de cada incendio utilizando la función fetch_environment.

    Parámetros:
    - fires: DataFrame que contiene información sobre los incendios, incluyendo lat_mean, lon_mean y date_first.
    - limit: número máximo de filas a procesar del DataFrame (por defecto es 20).
    - fecha_ini: fecha de inicio para filtrar los incendios (opcional).
    - fecha_fin: fecha de fin para filtrar los incendios (opcional).
    
    Devuelve:
    - DataFrame con las características físicas obtenidas para cada incendio.
    '''
    
    async with aiohttp.ClientSession() as session:
        ini = time.time()

        print("Comenzando extracción...")

        if limit == -1:
            rows = fires.to_dict('records')
        else:
            rows = fires.head(limit).to_dict('records')

        tareas = [
            fetch_environment(
                session=session,
                lat=row['lat_mean'],
                lon=row['lon_mean'],
                date=row['date_first'].split()[0],
                indice=i,
            )
            for i, row in enumerate(rows)
        ]
        resultados = await asyncio.gather(*tareas)
        final_df = pd.DataFrame(resultados)

        fin = time.time()

        print(f"Extraidas {len(final_df)} filas de características físicas en {fin - ini:.2f} segundos.")
        print(final_df.head(limit))

        minioFunctions.preguntar_subida(final_df, "grupo3/raw/Fisicas/")

        return final_df

