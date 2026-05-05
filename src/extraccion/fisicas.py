import asyncio
import time
from . import minioFunctions
import pandas as pd
import aiohttp
from . import interrupcion
from datetime import date as dt_date, datetime

sem_global = asyncio.Semaphore(5)
contador = 0
limit = 5000
sleep = 3600  

async def fetch_environment(session, lat, lon, date, indice=None, intentos=3, directo=False):
    '''
    Función que utiliza la API Open-Meteo para obtener características físicas.
    Detecta automáticamente si la fecha es histórica, futura o actual.
    
    :param session: Sesión de aiohttp para realizar la petición
    :param lat: Latitud
    :param lon: Longitud
    :param date: Fecha objetivo (str 'YYYY-MM-DD')
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

        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except Exception:
            target_date = dt_date.today()
            
        is_future = target_date >= dt_date.today()
        
        if is_future:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "daily": [
                    "temperature_2m_max", "temperature_2m_min",
                    "precipitation_sum", "wind_speed_10m_max",
                    "wind_gusts_10m_max", "shortwave_radiation_sum", 
                    "et0_fao_evapotranspiration", "sunshine_duration"
                ],
                "hourly": [
                    "temperature_2m", "relative_humidity_2m", 
                    "surface_pressure", "cloud_cover"
                ],
                "timezone": "UTC"
            }
        else:
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
                        
                        res = {
                            "lat": lat,
                            "lon": lon,
                            "date": date,
                            "temp_max": d["temperature_2m_max"][0],
                            "temp_min": d["temperature_2m_min"][0],
                            "precipitation": d["precipitation_sum"][0],
                            "wind_speed_max": d["wind_speed_10m_max"][0],
                            "wind_gusts_max": d["wind_gusts_10m_max"][0],
                            "radiation": d["shortwave_radiation_sum"][0],
                            "evapotranspiration": d["et0_fao_evapotranspiration"][0],
                            "sunshine_seconds": d["sunshine_duration"][0]
                        }

                        if is_future:
                            h = r.get("hourly", {})
                            res["temp_mean"] = sum(h.get("temperature_2m", [0])) / 24
                            res["humidity_mean"] = sum(h.get("relative_humidity_2m", [0])) / 24
                            res["pressure_mean"] = sum(h.get("surface_pressure", [0])) / 24
                            res["cloud_cover"] = sum(h.get("cloud_cover", [0])) / 24
                        else:
                            res["temp_mean"] = d["temperature_2m_mean"][0]
                            res["humidity_mean"] = d["relative_humidity_2m_mean"][0]
                            res["pressure_mean"] = d["surface_pressure_mean"][0]
                            res["cloud_cover"] = d["cloud_cover_mean"][0]
                        
                        if not is_future: await asyncio.sleep(0.5) 
                        return res
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

async def fetch_environment_batch(session, coords_list, date, intentos=3, directo=False):
    '''
    Extrae características físicas para múltiples coordenadas en una sola petición (batching).
    Optimiza drásticamente el tiempo de extracción y reduce el número de requests.

    :param session: Sesión de aiohttp
    :param coords_list: Lista de puntos (lat, lon)
    :param date: Fecha 
    :param intentos: Número de intentos 
    :param directo: Booleano para rate limit
    :return list: datos extraídos
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

        try:
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
        except Exception:
            target_date = dt_date.today()
            
        is_future = target_date >= dt_date.today()
        
        lats = [c['lat'] for c in coords_list]
        lons = [c['lon'] for c in coords_list]
        
        if is_future:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": ",".join(map(str, lats)),
                "longitude": ",".join(map(str, lons)),
                "start_date": date,
                "end_date": date,
                "daily": [
                    "temperature_2m_max", "temperature_2m_min",
                    "precipitation_sum", "wind_speed_10m_max",
                    "wind_gusts_10m_max", "shortwave_radiation_sum", 
                    "et0_fao_evapotranspiration", "sunshine_duration"
                ],
                "hourly": [
                    "temperature_2m", "relative_humidity_2m", 
                    "surface_pressure", "cloud_cover"
                ],
                "timezone": "UTC"
            }
        else:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": ",".join(map(str, lats)),
                "longitude": ",".join(map(str, lons)),
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
                    data = await response.json()
                    
                    if isinstance(data, dict) and data.get("error"):
                        error_msg = data.get("reason") or "Error desconocido de la API"
                        print(f"Error: Intento {i+1} fallido en batch de {len(coords_list)} puntos: {error_msg}")
                        if "limit exceeded" in error_msg.lower():
                            print(f"Limite de API alcanzado. Esperando 60 segundos antes de reintentar...")
                            await asyncio.sleep(60)
                        else:
                            await asyncio.sleep(2 * (i + 1))
                        continue
                    
                    if not isinstance(data, list):
                        data = [data]
                        
                    if len(data) != len(coords_list):
                        print(f"Advertencia: El numero de resultados devueltos ({len(data)}) no coincide con el batch ({len(coords_list)}).")
                    
                    resultados = []
                    for idx, coord in enumerate(coords_list):
                        lat = coord['lat']
                        lon = coord['lon']
                        
                        if idx < len(data) and "daily" in data[idx]:
                            d = data[idx]["daily"]
                            res = {
                                "lat": lat,
                                "lon": lon,
                                "date": date,
                                "temp_max": d["temperature_2m_max"][0],
                                "temp_min": d["temperature_2m_min"][0],
                                "precipitation": d["precipitation_sum"][0],
                                "wind_speed_max": d["wind_speed_10m_max"][0],
                                "wind_gusts_max": d["wind_gusts_10m_max"][0],
                                "radiation": d["shortwave_radiation_sum"][0],
                                "evapotranspiration": d["et0_fao_evapotranspiration"][0],
                                "sunshine_seconds": d["sunshine_duration"][0]
                            }
                            if is_future:
                                h = data[idx].get("hourly", {})
                                res["temp_mean"] = sum(h.get("temperature_2m", [0])) / 24
                                res["humidity_mean"] = sum(h.get("relative_humidity_2m", [0])) / 24
                                res["pressure_mean"] = sum(h.get("surface_pressure", [0])) / 24
                                res["cloud_cover"] = sum(h.get("cloud_cover", [0])) / 24
                            else:
                                res["temp_mean"] = d["temperature_2m_mean"][0]
                                res["humidity_mean"] = d["relative_humidity_2m_mean"][0]
                                res["pressure_mean"] = d["surface_pressure_mean"][0]
                                res["cloud_cover"] = d["cloud_cover_mean"][0]
                            resultados.append(res)
                        else:
                            error = {"lat": lat, "lon": lon, "date": date}
                            error.update({k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                                                    "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                                                    "radiation", "evapotranspiration", "sunshine_seconds"]})
                            resultados.append(error)
                    
                    print(f"Batch de {len(resultados)} puntos procesado correctamente (Request #{contador}).")
                    return resultados
                    
            except Exception as e:
                print(f"Error de conexion en batch: {e}")
                await asyncio.sleep(2 * (i + 1))
        
        print(f"Fallaron todos los intentos para el batch de {len(coords_list)} puntos.")
        resultados = []
        for coord in coords_list:
            error = {"lat": coord['lat'], "lon": coord['lon'], "date": date}
            error.update({k: None for k in ["temp_mean", "temp_max", "temp_min", "humidity_mean", "precipitation",
                                    "wind_speed_max", "wind_gusts_max", "pressure_mean", "cloud_cover",
                                    "radiation", "evapotranspiration", "sunshine_seconds"]})
            resultados.append(error)
        return resultados

async def df_fisicas(fires, limit=20, fecha_ini=None, fecha_fin=None, directo=False, pipeline=False, anio=None):
    '''
    Extrae características físicas de forma optimizada mediante batching.

    :param fires: DataFrame con los puntos de incendio
    :param limit: Límite de puntos a procesar (-1 para todos)
    :param fecha_ini: Fecha inicial de filtrado
    :param fecha_fin: Fecha final de filtrado
    :param directo: Booleano para control de rate limit
    :param pipeline: Booleano para ejecución en pipeline (subida automática)
    :param anio: Año correspondiente a los datos
    :return pd.DataFrame: DataFrame con las características físicas extraídas
    '''
    global contador
    contador = 0 
    
    if fecha_fin and fecha_ini: 
        fires = fires[fires.date.between(fecha_ini, fecha_fin)]

    if limit != -1:
        fires = fires.head(limit)

    fires = fires.copy()
    fires['date_str'] = fires['date'].astype(str).str.split().str[0]
    grupos = fires.groupby('date_str')

    session = aiohttp.ClientSession()
    resultados = []
    try:
        ini = time.time()
        print(f"Iniciando extracción optimizada para {len(fires)} puntos...")

        tareas = []
        for fecha_str, grupo in grupos:
            coords = grupo[['lat', 'lon']].to_dict('records')
            for i in range(0, len(coords), 50):
                batch = coords[i:i+50]
                tareas.append(fetch_environment_batch(session, batch, fecha_str, directo=directo))

        try:
            for tarea in asyncio.as_completed(tareas):
                try:
                    res_batch = await tarea
                    resultados.extend(res_batch)
                except asyncio.CancelledError:
                    print("\n Interrupción detectada. Guardando resultados parciales...")
                    final_df = pd.DataFrame(resultados)
                    interrupcion.guardar_parcial(final_df, prefijo="fisicas_parcial")
                    for t in tareas:
                        if not t.done(): t.cancel()
                    raise
        except KeyboardInterrupt:
            print("\n Interrupción detectada. Guardando resultados parciales...")
            final_df = pd.DataFrame(resultados)
            interrupcion.guardar_parcial(final_df, prefijo="fisicas_parcial")
            raise
            
        final_df = pd.DataFrame(resultados)
    finally:
        await session.close()

    fin = time.time()
    print(f"Extraídas {len(final_df)} filas en {fin - ini:.2f} segundos.")
    print(f"Total de requests: {contador}")
    
    if pipeline:
        assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
        cliente = minioFunctions.crear_cliente()
        minioFunctions.subir_fichero(cliente, f"grupo3/raw/Fisicas/Fisicas_{anio}.parquet", final_df)
    else:
        minioFunctions.preguntar_subida(final_df, "grupo3/raw/Fisicas/")
    
    return final_df