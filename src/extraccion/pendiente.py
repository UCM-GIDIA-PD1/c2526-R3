import numpy as np
import ee
import asyncio
import time
from . import minioFunctions
from . import interrupcion
import pandas as pd

sem_global = asyncio.Semaphore(10)

async def pendiente(lat, lon, date, indice = None): #Ignacio: añadido date
  '''
    Calcula la elevacion y pendiente (en grados y porcentaje) de un punto usando Google Earth Engine.
    
    Importante:
    - Utiliza el dataset MERIT/DEM/v1_0_3.
    - Asume que Earth Engine siempre devolvera un diccionario con las claves 'dem' y 'slope'.
    - Si el valor de 'slope' es vacio o 0, el calculo del porcentaje asume 0 por defecto.
    
    :param lat: Latitud
    :param lon: Longitud
    :param date: Fecha
    :param indice: Índice opcional para seguimiento
    :return dict: Diccionario con la elevación y pendientes
  '''
  async with sem_global:
    elev = ee.Image('MERIT/DEM/v1_0_3').select('dem')
    punto = ee.Geometry.Point([lon, lat])

    slope = ee.Terrain.slope(elev)

    data = elev.addBands(slope).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=punto,
        scale=30
    )

    loop = asyncio.get_event_loop()
    res = await loop.run_in_executor(None, lambda: data.getInfo())
    if indice is not None:
      print(f"Pendiente {indice} extraida.")
    
    return {
        "lat" : lat,
        "lon" : lon, 
        "date" : date,
        "elevacion_centro": res.get('dem'),
        "grados": res.get('slope'),
        "porcentaje": (np.tan(np.radians(res['slope'])) * 100) if res['slope'] else 0
    }

async def df_pendiente(fires, limit = 20, fecha_ini = None, fecha_fin = None):
  
    '''
    Extrae la informacion del terreno de una serie de incendios 
    
    Requiere que el DataFrame fires contenga las columnas 'lat', 'lon' y 'date'.
    
    :param fires: DataFrame con los datos
    :param limit: Límite de filas a procesar 
    :param fecha_ini: Fecha inicial 
    :param fecha_fin: Fecha final
    :return pd.DataFrame: DataFrame final 
    '''

    ini = time.time()

    print("Comenzando extracción...")

    if limit == -1:
        rows = fires.to_dict('records')
    else:
        rows = fires.head(limit).to_dict('records')

    tareas = [
        pendiente(
            row['lat'],
            row['lon'],
            row['date'],
            indice=i
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
                if resultados:
                    final_df = pd.DataFrame(resultados)
                    interrupcion.guardar_parcial(final_df, prefijo="pendiente_parcial")
                else:
                    print("No hay datos parciales para guardar.")
                # Cancelar tareas pendientes
                for t in tareas:
                    if not t.done():
                        t.cancel()
                raise
    except KeyboardInterrupt:
        print("\n Interrupción detectada. Guardando resultados parciales...")
        if resultados:
            final_df = pd.DataFrame(resultados)
            interrupcion.guardar_parcial(final_df, prefijo="pendiente_parcial")
        else:
            print("No hay datos parciales para guardar.")
        raise
    except Exception as e:
        print(f"Error durante la extracción: {e}")
        raise

    final_df = pd.DataFrame(resultados)

    fin = time.time()

    print(f"Extraidas {len(final_df)} filas de pendiente en {fin - ini:.2f} segundos.")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Pendiente/")

    return final_df