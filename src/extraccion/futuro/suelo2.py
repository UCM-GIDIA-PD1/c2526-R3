import asyncio
import aiohttp
import pandas as pd
import time
from datetime import datetime
import numpy as np
from extraccion import interrupcion
from extraccion import minioFunctions

sem_global = asyncio.Semaphore(10)

def formatear_fecha(fecha):

    if isinstance(fecha, (pd.Timestamp, datetime)):
        return fecha.strftime('%Y-%m-%d')
    dt = pd.to_datetime(fecha)
    return dt.strftime('%Y-%m-%d')


async def soil_temp(lat, lon, fecha_ini, fecha_fin, indice):
    fecha_ini = formatear_fecha(fecha_ini)
    fecha_fin = formatear_fecha(fecha_fin)
    if fecha_ini is None or fecha_fin is None:
        print(f"Fechas inválidas para ({lat}, {lon}): {fecha_ini} - {fecha_fin}")
        return []

    async with sem_global:
        base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        params = {
            "parameters": "TSOIL1",
            "community": "ag",
            "longitude": lon,
            "latitude": lat,
            "start": fecha_ini.replace("-", ""),
            "end": fecha_fin.replace("-", ""),
            "format": "JSON",
            "user": "test123"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(base_url, params=params, timeout=30) as resp:
                    if resp.status != 200:
                        print(f"Error {resp.status} en ({lat}, {lon})")
                        return []
                    data = await resp.json()
        except Exception as e:
            print(f"Excepción en ({lat}, {lon}): {e}")
            return []
        finally:
            await asyncio.sleep(0.3)

    try:
        ts_dict = data.get('properties', {}).get('parameter', {}).get('TSOIL1', {})
        if not ts_dict:
            ts_dict = data.get('TSOIL1', {})

        if not ts_dict:
            print(f"No hay datos de TSOIL1 para ({lat}, {lon})")
            return []

        resultados = []
        for fecha_str, temp in ts_dict.items():
            if temp is None:
                continue
            fecha = datetime.strptime(fecha_str, "%Y%m%d").strftime("%Y-%m-%d")
            resultados.append({
                'fire_index': indice,
                'lat': lat,
                'lon': lon,
                'date': fecha,
                'soil_temp': temp
            })
        if indice is not None:
            print(f"Temperatura suelo {indice} extraída.")
        return resultados
    except Exception as e:
        print(f"Error procesando respuesta para ({lat}, {lon}): {e}")
        return []

async def df_soil_temp(fires, limit = 20, fecha_ini = None, fecha_fin = None):
    inicio = time.time()
    print("Iniciando extracción de temperatura del suelo...")

    if limit == -1:
        rows = fires.to_dict('records')
        print(f"Procesando todas las {len(rows)} filas")
    else:
        rows = fires.head(limit).to_dict('records')
        print(f"Procesando {len(rows)} filas (limit={limit})")

    rango = fecha_ini is not None and fecha_fin is not None

    tareas = []
    for i, row in enumerate(rows):
        if rango:
            ini_fila = fecha_ini
            fin_fila = fecha_fin
        else:
            ini_fila = row['date_first']
            fin_fila = row['date_first']

        tareas.append(
            soil_temp(
                lat=row['lat_mean'],
                lon=row['lon_mean'],
                fecha_ini=ini_fila,
                fecha_fin=fin_fila,
                indice=i
            )
        )

    resultados_por_fila = []
    try:
        for tarea in asyncio.as_completed(tareas):
            try:
                resultados_por_fila.append(await tarea)
            except asyncio.CancelledError:
                print("\n Interrupción detectada. Guardando resultados parciales...")
                todos = []
                for lista in resultados_por_fila:
                    if isinstance(lista, list):
                        todos.extend(lista)
                if todos:
                    df_resultado = pd.DataFrame(todos)
                    interrupcion.guardar_parcial(df_resultado, prefijo="suelo_parcial")
                else:
                    print("No hay datos parciales para guardar.")
                for t in tareas:
                    if not t.done():
                        t.cancel()
                raise
    except KeyboardInterrupt:
        print("\n Interrupción detectada. Guardando resultados parciales...")
        todos = []
        for lista in resultados_por_fila:
            if isinstance(lista, list):
                todos.extend(lista)
        if todos:
            df_resultado = pd.DataFrame(todos)
            interrupcion.guardar_parcial(df_resultado, prefijo="suelo_parcial")
        else:
            print("No hay datos parciales para guardar.")
        raise

    todos = []
    for lista in resultados_por_fila:
        todos.extend(lista)

    df_resultado = pd.DataFrame(todos)

    fin = time.time()
    print(f"Tiempo total: {fin - inicio:.2f} segundos.")

    if not df_resultado.empty:
        nulos_por_columna = df_resultado.isnull().sum()
        print("\nValores nulos por columna:")
        print(nulos_por_columna[nulos_por_columna > 0] if any(nulos_por_columna) else "No hay nulos.")
    else:
        print("DataFrame vacío, no hay datos.")

    csv_filename = "soil_temperatures.csv"
    minioFunctions.preguntar_subida(df_resultado, "grupo3/raw/Suelo2/")

    df_resultado.to_csv(csv_filename, index=False)
    print(f"\nResultados guardados en '{csv_filename}'")

    return df_resultado