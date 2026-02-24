import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPoint
from extraccion import minioFunctions
import datetime

# Funciones encargadas transformar y limpiar los diferentes .csv de incendios en DataFrames utilizables y útiles

def limpieza(df):

    '''
    Limpia el DataFrame de incendios eliminando filas con baja confianza, duplicados y filtrando por tipo de incendio.
    Parámetros: dataFrame de incendios con columnas 'latitude', 'longitude', 'acq_date', 'confidence', 'frp' y 'type'.
    Devuelve: DataFrame limpio con columnas 'lat', 'lon', 'frp' y 'date'.
    '''

    assert not df.empty, "No se pueden analizar incendios, el DataFrame esta vacio"
    df['acq_date'] = pd.to_datetime(df['acq_date'])
    df = df[df['confidence'] != 'l']
    df = df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date'])
    df= df[df['frp'] > 50]
    df = df[df['type'] == 0]
    df = df.rename(columns={
            'latitude': 'lat',
            'longitude': 'lon',
            'acq_date': 'date_first'
    })
    columnas_utiles = ['lat', 'lon', 'frp', 'date_first']
    return df[columnas_utiles]

def calcular_area_incendios(df, pixel_res_meters=1000):
    """
    Calcula el área de los incendios agrupados por fire_id.
    
    Parámetros:
    - df: DataFrame con columnas 'lon', 'lat' y 'fire_id'.
    - pixel_res_meters: Resolución del sensor en metros (1000 para MODIS, 375 para VIIRS).
    
    Retorna:
    - DataFrame con fire_id y el área calculada en km².
    """
    if df.empty:
        return pd.Series(dtype='float64', name='area_ha')
    
    gdf = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df['lon'], df['lat']), 
        crs="EPSG:4326"
    )
    
    gdf_proj = gdf.to_crs("EPSG:6933")
    
    resultados = []
    
    for fire_id, grupo in gdf_proj.groupby('fire_id'):
        num_puntos = len(grupo)
        
        if num_puntos >= 3:
            geometria_base = MultiPoint(grupo.geometry.tolist()).convex_hull
            poligono_final = geometria_base.buffer(pixel_res_meters / 2)
            area_m2 = poligono_final.area
            
        else:
            area_m2 = num_puntos * (pixel_res_meters ** 2)
            
        area_ha = area_m2 / 10000
        
        resultados.append({
            'fire_id': fire_id,
            'puntos_activos': num_puntos,
            'area_ha': round(area_ha, 2)
        })
        
    area = pd.DataFrame(resultados).sort_values(by='puntos_activos', ascending=False)
    return area[['fire_id', 'area_ha']]

def separate_fire_events(df, dist_km=2.0, mes_inicial=1, mes_final=12):

    """
    Asigna un ID único a cada grupo de puntos que pertenezcan al mismo incendio.

    Parámetros:
    - df: DataFrame de FIRMS.
    - dist_km: Distancia máxima para considerar que dos puntos son del mismo incendio.
    Devuelve:
    - df: DataFrame con una nueva columna 'fire_id' que identifica cada incendio.
    - resumen: DataFrame con un resumen de cada incendio, incluyendo su ubicación media, suma y media de FRP, cantidad de puntos, fecha del primer y último punto, y duración en días.
    """

    assert not df.empty, "El DataFrame contenia fuegos poco relevantes y se vacio, no se pueden separar eventos de incendios"

    # 1. Convertir coordenadas a radianes para usar con la métrica haversine
    coords = np.radians(df[['lat', 'lon']])

    # 2. Configurar el algoritmo
    # El radio de la Tierra es ~6371 km. eps = distancia / radio_tierra
    kms_per_radian = 6371.0
    epsilon = dist_km / kms_per_radian

    db = DBSCAN(eps=epsilon, min_samples=1, metric='haversine').fit(coords)

    # 3. Asignar las etiquetas al DataFrame
    df['fire_id'] = db.labels_.astype(str)
    

    # Diferenciar incendios que ocurren en el mismo lugar pero con una diferencia de tiempo mayor a 6 dias, asignando un nuevo ID a cada uno de ellos

    for incendio in df['fire_id'].unique():
        incendio_df = df[df['fire_id'] == incendio]
        ultimo_id = incendio
        ultima_fecha = datetime.datetime(2020, 1, 1) #fecha de ejemplo que siempre es menor que la fecha de los incendios 
            
        for captacion in incendio_df.itertuples():
            dif = (captacion.date - ultima_fecha).days

            if dif > 6:
                df.loc[captacion.Index, 'fire_id'] = f'{ultimo_id}_{captacion.Index}'
                ultimo_id = f'{ultimo_id}_{captacion.Index}'
            else:
                df.loc[captacion.Index, 'fire_id'] = ultimo_id
                
            ultima_fecha = captacion.date

    # Opcional: Contar cuántos puntos hay por incendio y su FRP total
        resumen = df.groupby('fire_id').agg({
            'lat': 'mean',
            'lon': 'mean',
            'frp': ['sum', 'mean', 'count'],
            'date': ['first', 'last']
        })

        df['lat'] = df['lat'].round(2)
        df['lon'] = df['lon'].round(2)

        resumen.columns = ['lat_mean', 'lon_mean', 'frp_sum', 'frp_mean', 'count', 'date_first', 'date_last']

        resumen['duration_days'] = (resumen['date_last'] - resumen['date_first']).dt.days + 1

    return df, resumen

'''
def crear_parquet(df, filename='resumen_incendios.parquet'):
    """
    Guarda el DataFrame en un archivo Parquet.
    
    Parámetros:
    - df: DataFrame a guardar.
    - filename: Nombre del archivo Parquet (con extensión .parquet).
    """
    df.to_parquet(filename, index=False)
'''

def fetch_fires(df_clean, fecha_ini = None, fecha_fin = None, question=False, limpiar = True):

    """
    Funcion que procesa un DataFrame de incendios, limpiándolo, separando los eventos de incendio y calculando el área de cada incendio
    Parámetros:
    - df: DataFrame con los datos de incendios.
    - round_decimals: número de decimales a redondear para las coordenadas (por defecto 2).
    - fecha_ini: fecha inicial del rango de incendios a procesar (por defecto None).
    - fecha_fin: fecha final del rango de incendios a procesar (por defecto None).
    - question: booleano que indica si se debe preguntar al usuario si quiere subir el resumen a MinIO (por defecto False).
    Devuelve resumen con la información relevante de cada incendio, incluyendo:
    -   LAT_MEAN: la media de las latitudes de los puntos que pertenecen al mismo incendio
    -   LON_MEAN: la media de las longitudes de los puntos que pertenecen al mismo incendio
    -   FRP - sum, mean: la suma y media de los FRP de los puntos que pertenecen al mismo incendio
    -   COUNT indica la cantidad de puntos que pertenecen al mismo incendio
    -   DATE_FIRST: la fecha del primer punto del incendio
    -   DATE_LAST: la fecha del ultimo punto del incendio
    -   DURATION_DAYS: la duracion del incendio en dias
    -   AREA_HA: el area del incendio en hectareas
    """

    #cliente = minioFunctions.crear_cliente()
    #df = minioFunctions.bajar_fichero(cliente, 'grupo3/raw/incendios/j1-viirs-22-26.parquet', type="df")
    if limpiar:
        df_clean = limpieza(df_clean)

    if fecha_ini is not None:
        fecha_ini = pd.to_datetime(fecha_ini)
        df_clean = df_clean[pd.to_datetime(df_clean['date_first']) >= fecha_ini]
    
    if fecha_fin is not None:
        fecha_fin = pd.to_datetime(fecha_fin)
        df_clean = df_clean[pd.to_datetime(df_clean['date_first']) <= fecha_fin]

    if df_clean.empty:
        print("No hay incendios en el rango de fechas seleccionado.")
        return pd.DataFrame()
    print("Df separado")

    if limpiar:
        df_clean, resumen = separate_fire_events(df_clean, 5.0)
    
    if limpiar:
        areas_df = calcular_area_incendios(df_clean, pixel_res_meters=375) 

        resumen = resumen.merge(areas_df, on='fire_id', how='left')
        print("Hectáreas calculadas")
    else:
        resumen = df_clean.copy()

    if question:
        minioFunctions.preguntar_subida(resumen.sort_values(by='count', ascending=False), "grupo3/raw/incendios/")
    
    return resumen.sort_values(by='count', ascending=False)