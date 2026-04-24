import asyncio
import aiohttp
import pandas as pd
import time
from datetime import datetime
import numpy as np
from .. import interrupcion
from .. import minioFunctions

sem_global = asyncio.Semaphore(10)

def formatear_fecha(fecha):
    """
    Formatea una fecha a string YYYY-MM-DD.
    
    :param fecha: Fecha en formato pd.Timestamp, datetime o string
    :return str: Fecha formateada
    """
    if isinstance(fecha, (pd.Timestamp, datetime)):
        return fecha.strftime('%Y-%m-%d')
    dt = pd.to_datetime(fecha)
    return dt.strftime('%Y-%m-%d')


async def soil_temp(lat, lon, date, indice):
    """
    Extrae la temperatura del suelo de la API de NASA POWER para unas coordenadas y fecha dadas.
    
    :param lat: Latitud
    :param lon: Longitud
    :param date: Fecha objetivo
    :param indice: Índice del procesamiento
    :return list: Lista con un diccionario de resultados o vacía si falla
    """
    date_fmt = formatear_fecha(date)
    if date_fmt is None:
        print(f"Fecha inválida para ({lat}, {lon}): {date}")
        return []

    async with sem_global:
        base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
        params = {
            "parameters": "TSOIL1",
            "community": "ag",
            "longitude": lon,
            "latitude": lat,
            "start": date_fmt.replace("-", ""),
            "end": date_fmt.replace("-", ""),
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
            await asyncio.sleep(5)

    try:
        ts_dict = data.get('properties', {}).get('parameter', {}).get('TSOIL1', {})
        if not ts_dict:
            ts_dict = data.get('TSOIL1', {})

        if not ts_dict:
            print(f"No hay datos de TSOIL1 para ({lat}, {lon})")
            return []

        fecha_objetivo_str = date_fmt.replace("-", "")

        resultados = []
        temp = ts_dict.get(fecha_objetivo_str)
        if temp is not None:
            resultados.append({
                'fire_index': indice,
                'lat': lat,
                'lon': lon,
                'date': date_fmt,
                'soil_temp': temp
            })

        if indice is not None:
            print(f"Temperatura suelo {indice} extraída.")
        return resultados
    except Exception as e:
        print(f"Error procesando respuesta para ({lat}, {lon}): {e}")
        return []


async def df_soil_temp(fires, limit=20, fecha_ini=None, fecha_fin=None, pipeline=False, anio=None):
    """
    Se extraen los datos de temperatura del suelo para un dataset
    
    Requiere que el DataFrame fires contenga las columnas 'lat', 'lon' y 'date' (o 'date_first')
    
    :params fires: Dataframe con los puntos
    :params limit: Límite de filas
    :params fecha_ini: Fecha de inicio
    :params fecha_fin: Fecha de fin
    :param pipeline: si es true se automatiza la subida a Minio sin preguntar (por defecto False)
    :param anio: Año para subir el archivo a Minio automáticamente
    :return pd.DataFrame: DataFrame final
    """
    inicio = time.time()
    print("Iniciando extracción de temperatura del suelo...")

    # Normalizamos la columna de fecha por si viene como 'date_first'
    if 'date' not in fires.columns and 'date_first' in fires.columns:
        fires = fires.rename(columns={'date_first': 'date'})

    fin_none = fecha_fin is None
    ini_none = fecha_ini is None

    # Se filtra el DataFrame
    if not fin_none and not ini_none: 
        fires = fires[fires['date'].between(fecha_ini, fecha_fin)]

    if limit != -1:
        fires = fires.head(limit)   
        print(f"Procesando {len(fires)} filas (limit={limit})")
    else:
        print(f"Procesando todas las {len(fires)} filas")
        
    fires = fires.reset_index(drop=True)
    rows = fires.to_dict('records')

    tareas = []
    for i, row in enumerate(rows):
        tareas.append(
            soil_temp(
                lat=row['lat'],
                lon=row['lon'],
                date=row['date'],
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
        for t in tareas:
            if not t.done():
                t.cancel()
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

    if pipeline:
        assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
        cliente = minioFunctions.crear_cliente()
        minioFunctions.subir_fichero(cliente, f"grupo3/raw/Suelo2/Suelo2_{anio}.parquet", df_resultado)
    else:
        minioFunctions.preguntar_subida(df_resultado, "grupo3/raw/Suelo2/")

    df_resultado.to_csv(csv_filename, index=False)
    print(f"\nResultados guardados en '{csv_filename}'")

    return df_resultado