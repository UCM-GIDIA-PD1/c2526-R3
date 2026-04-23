import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPoint
from . import minioFunctions
from . import interrupcion
import datetime

# Funciones encargadas transformar y limpiar los diferentes .csv de incendios en DataFrames utilizables y útiles

def limpieza(df):
    '''
    Limpia el DataFrame de incendios eliminando filas con baja confianza, duplicados y filtrando por tipo de incendio.
    
    :param df: DataFrame de incendios con columnas 'latitude', 'longitude', 'acq_date', 'confidence', 'frp' y 'type'
    :return pd.DataFrame: DataFrame limpio con columnas 'lat', 'lon', 'frp' y 'date'
    '''

    assert not df.empty, "No se pueden analizar incendios, el DataFrame esta vacio"
    df['acq_date'] = pd.to_datetime(df['acq_date'])
    df = df[df['confidence'] != 'l']
    df = df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date'])
    df = df[df['frp'] > 50]

    df = df.rename(columns={
            'latitude': 'lat',
            'longitude': 'lon',
            'acq_date': 'date'
    })
    columnas_utiles = ['lat', 'lon', 'frp', 'date']

    return df[columnas_utiles]

def calcular_area_incendios(df, pixel_res_meters=1000):
    """
    Calcula el área de los incendios agrupados por fire_id.
    
    :param df: DataFrame con columnas 'lon', 'lat' y 'fire_id'
    :param pixel_res_meters: Resolución del sensor en metros (1000 para MODIS, 375 para VIIRS)
    :return pd.DataFrame: DataFrame con fire_id y el área calculada en km²
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

    :param df: DataFrame de FIRMS
    :param dist_km: Distancia máxima para considerar que dos puntos son del mismo incendio
    :param mes_inicial: Mes inicial
    :param mes_final: Mes final
    :return tuple: DataFrame con una nueva columna 'fire_id' y un DataFrame resumen del incendio
    """

    assert not df.empty, "El DataFrame contenia fuegos poco relevantes y se vacio, no se pueden separar eventos de incendios"

    df = df[df['date'].dt.month.between(mes_inicial, mes_final)].copy()

    if df.empty:
        return df, pd.DataFrame()

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

        resumen.columns = ['lat', 'lon', 'frp_sum', 'frp_mean', 'count', 'date', 'date_last']

        resumen['duration_days'] = (resumen['date_last'] - resumen['date']).dt.days + 1

    return df, resumen

def fetch_fires(df, fecha_ini = None, fecha_fin = None, question=False, pipeline=False, anio = None):

    """
    Función que procesa un DataFrame de incendios, limpiándolo, separando los eventos de incendio y calculando el área de cada incendio.
    
    :param df: DataFrame con los datos de incendios
    :param fecha_ini: Fecha inicial del rango de incendios a procesar (por defecto None)
    :param fecha_fin: Fecha final del rango de incendios a procesar (por defecto None)
    :param question: Booleano que indica si se debe preguntar al usuario si quiere subir el resumen a MinIO (por defecto False)
    :param pipeline: Booleano que indica si se está ejecutando en un pipeline (por defecto False)
    :param anio: Año para subir el archivo a MinIO automáticamente (requerido si pipeline es True)
    :return pd.DataFrame: DataFrame resumen con la información relevante de cada incendio (lat, lon, FRP, COUNT, date, DATE_LAST, DURATION_DAYS, AREA_HA)
    """

    try:
        resumen = None
        df_clean = limpieza(df)

        if fecha_ini is not None:
            fecha_ini = pd.to_datetime(fecha_ini)
            df_clean = df_clean[df_clean['date'] >= fecha_ini]
        
        if fecha_fin is not None:
            fecha_fin = pd.to_datetime(fecha_fin)
            df_clean = df_clean[df_clean['date'] <= fecha_fin]

        if df_clean.empty:
            print("No hay incendios en el rango de fechas seleccionado.")
            return pd.DataFrame()
        print("Df separado")
        df_clean, resumen = separate_fire_events(df_clean, 5.0)
        
        areas_df = calcular_area_incendios(df_clean, pixel_res_meters=375) 

        resumen = resumen.merge(areas_df, on='fire_id', how='left')
        print("Hectáreas calculadas")

        df_clean = df_clean.rename(columns={
        'lat_mean': 'lat',
        'lon_mean': 'lon',
        'date_first': 'date'
        })
        
    except KeyboardInterrupt:
        print("\n Interrupción detectada. Guardando resultados parciales...")
        if 'resumen' in locals() and not resumen.empty:
            interrupcion.guardar_parcial(resumen, prefijo="incendios_parcial")
        else:
            print("No hay datos parciales para guardar.")
        raise

    if question or not pipeline:
        minioFunctions.preguntar_subida(resumen.sort_values(by='count', ascending=False), "grupo3/raw/incendios/")
    else:
        assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
        cliente = minioFunctions.crear_cliente()
        minioFunctions.subir_fichero(cliente, f"grupo3/raw/incendios/incendios_{anio}.parquet", resumen.sort_values(by='count', ascending=False))
    return resumen.sort_values(by='count', ascending=False)
        