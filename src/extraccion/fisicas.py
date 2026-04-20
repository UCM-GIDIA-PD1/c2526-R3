import asyncio
import time
from . import minioFunctions
import pandas as pd
import aiohttp
from . import interrupcion

sem_global = asyncio.Semaphore(5)
contador = 0
limit = 5000
sleep = 3600  

async def fetch_environment(session, lat, lon, date, indice=None, intentos=3, directo=False):
    '''
    Función que utiliza la API Open-Meteo para obtener características físicas    
    
    :param session: Sesión de aiohttp para realizar la petición
    :param lat: Latitud
    :param lon: Longitud
    :param date: Fecha objetivo
    :param indice: Índice del procesamiento actual
    :param intentos: Número de intentos en caso de fallo de conexión
    :param directo: Booleano para activar el control de límite de peticiones (Rate Limit)
    :return dict: Diccionario con los datos físicos extraídos o valores nulos si falla
    '''
    
    global contador
    async with sem_global:
        if directo:
            contador += 1
            if contador % limit == 0:  
                print(f"Límite de requests alcanzado. Durmiendo 1 hora...")
                await asyncio.sleep(sleep)
            elif contador > limit * 100:  
                contador = 1

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
                        print(f"Características físicas {indice} extraidas. Request #{contador}")
                        await asyncio.sleep(3)
                        return {
                            "lat": lat,
                            "lon": lon,
                            "date": date,
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

        error = {"lat": lat, "lon": lon, "date": date}
        error.update({k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                                "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                                "radiation", "evapotranspiration", "sunshine_seconds"]})
        return error

async def df_fisicas(fires, limit=20, fecha_ini=None, fecha_fin=None, directo=False):
    '''
    Función que extrae características físicas, cabe resaltar sus límites de 5000 por hora y 10000 diarios.

    :param fires: DataFrame con los puntos (debe contener 'lat', 'lon', y 'date')
    :param limit: Límite de filas a procesar (-1 para procesar todo)
    :param fecha_ini: Fecha de inicio para filtrar el DataFrame
    :param fecha_fin: Fecha de fin para filtrar el DataFrame
    :param directo: Booleano para activar el control de límite de peticiones
    :return pd.DataFrame: DataFrame final con los datos físicos añadidos
    '''
    
    global contador
    contador = 0 
    
    fin_none = fecha_fin is None
    ini_none = fecha_ini is None

    if not fin_none and not ini_none: 
        fires = fires[fires.date.between(fecha_ini, fecha_fin)]

    session = aiohttp.ClientSession()
    try:
        ini = time.time()
        print("Comenzando extracción...")

        if limit == -1:
            rows = fires.to_dict('records')
        else:
            rows = fires.head(limit).to_dict('records')

        tareas = [
            fetch_environment(
                session=session,
                lat=row['lat'],
                lon=row['lon'],
                date=row['date'].split()[0],
                indice=i,
                directo=directo
            )
            for i, row in enumerate(rows)
        ]

        resultados = []
        try:
            for tarea in asyncio.as_completed(tareas):
                try:
                    resultados.append(await tarea)
                except asyncio.CancelledError:
                    print("\n Interrupción detectada. Guardando resultados parciales...")
                    final_df = pd.DataFrame(resultados)
                    interrupcion.guardar_parcial(final_df)
                    for t in tareas:
                        if not t.done():
                            t.cancel()
                    await asyncio.sleep(0.1)
                    raise 
        except KeyboardInterrupt:
            print("\n Interrupción detectada. Guardando resultados parciales...")
            final_df = pd.DataFrame(resultados)
            interrupcion.guardar_parcial(final_df)
            for t in tareas:
                if not t.done():
                    t.cancel()
            await asyncio.sleep(0.1)
            raise
        except Exception as e:
            print(f"Error durante la extracción: {e}")
            raise
        else:
            final_df = pd.DataFrame(resultados)
    finally:
        await session.close()

    fin = time.time()
    print(f"Extraídas {len(final_df)} filas de características físicas en {fin - ini:.2f} segundos.")
    print(f"Total de requests realizados: {contador}")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Fisicas/")
    return final_df