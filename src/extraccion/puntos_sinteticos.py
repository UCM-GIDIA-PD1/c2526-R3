import geopandas as gpd # Para GeoDataFrame y GeoSeries
import pandas as pd
from shapely.geometry import Point # Para crear puntos
from pyproj import Transformer
import numpy as np
import rasterio
from dotenv import load_dotenv
from . import minioFunctions
from . import filtros_no_sinteticos

# Funciones encargadas de crear puntos sintéticos de acuerdo con el Área, el frp de fuego y el número de incendios
def crearAleatorios(mascara, parquetAnio, noIncendios, anio, src, transformer):

    '''
    Crea puntos aleatorios dentro de una máscara geográfica, asegurando que sean válidos según ciertos filtros.
    Parámetros:
    - mascara: ruta al archivo de la máscara geográfica (archivo Parquet con geometría).
    - parquetAnio: DataFrame con los incendios del año (para validar puntos).
    - noIncendios: número total de puntos de no incendio a generar en esta zona (debe ser múltiplo de 12).
    - anio: año para asignar a los puntos generados.
    - src: objeto raster para validar puntos.
    - transformer: objeto Transformer para validar puntos.
    Devuelve:
    - tres listas: latitudes, longitudes y fechas de los puntos generados.
    '''

    listaLat = []
    listaLon = []
    fechas = []

    if noIncendios % 12 != 0:
        noIncendios = ((noIncendios // 12) + 1) * 12

    puntos_por_mes = noIncendios // 12  # número de puntos a generar cada mes en esta zona
    cliente = minioFunctions.crear_cliente()

    # Leer la máscara de la región
    mascara_gdf = minioFunctions.bajar_fichero(cliente, mascara, "gdf")
    mascara_gdf = mascara_gdf.to_crs("EPSG:4326")
    # Obtener los límites geográficos de la región
    minx, miny, maxx, maxy = mascara_gdf.geometry.total_bounds

    #Evitamos que haya errores
    minx = max(minx, -10.0)
    maxx = min(maxx, 30.0)
    miny = max(miny, 35.0)
    maxy = min(maxy, 71.0)

    # Generar puntos para cada mes (de 1 a 12)
    for mes in range(1, 13):
        for _ in range(puntos_por_mes):
            
            #Tres intentos de conseguir un mejor punto
            intentos = 0

            while intentos < 3:
                lat = np.random.uniform(miny, maxy)
                lon = np.random.uniform(minx, maxx)
                intentos += 1

                if filtros_no_sinteticos.puntoValido(lat, lon, parquetAnio, src, transformer):
                    listaLat.append(lat)
                    listaLon.append(lon)
                    dia = np.random.randint(1, 29)  # día aleatorio entre 1 y 28 (incluido)
                    fechas.append(filtros_no_sinteticos.crearFecha(dia, mes, anio))
                    break  

    return listaLat, listaLon, fechas


def crearCercanos(incendiosZona, numNoIncendios, frpTotal, parquetAnio, src, transformer):

    """
    Crea puntos sintéticos cercanos a incendios reales, distribuidos proporcionalmente al FRP de cada incendio.

    Parámetros:
    - incendiosZona: DataFrame con los incendios de la zona (ya filtrados, NO una ruta)
    - numNoIncendios: número total de puntos de no incendio a generar en esta zona
    - frpTotal: suma de frp_mean de todos los incendios de la zona
    - parquetAnio: DataFrame con todos los incendios del año (para validar puntos)
    - src, transformer: objeto raster y objeto Transformer para validar puntos

    Devuelve:
    - numNoIncendios_restante: número de puntos que no se han podido generar cerca de incendios (para generar aleatorios)
    - tres listas: latitudes, longitudes y fechas de los puntos generados.
    """

    from math import cos, sin
    import numpy as np
    import pandas as pd
    

    df = incendiosZona
    listaLat = []
    listaLon = []
    fechas = []

    if len(df) == 0 or frpTotal == 0:
        return numNoIncendios, listaLat, listaLon, fechas

    numNoIncendios_restante = numNoIncendios

    for i in range(len(df)):
        fila = df.iloc[i]
        importancia = fila['frp_mean'] / frpTotal
        numPuntos = round(importancia * numNoIncendios)

        if numPuntos > 0:
            numNoIncendios_restante -= numPuntos
            puntos_por_mes = numPuntos // 12
            resto = numPuntos % 12

            fecha_incendio = pd.to_datetime(fila['date_first'])
            mes_incendio = fecha_incendio.month
            anio_incendio = fecha_incendio.year
            dia_base = min(fecha_incendio.day, 28)

            for mes in range(1, 13):
                puntos_este_mes = puntos_por_mes + (1 if mes <= resto else 0)

                if mes == mes_incendio:
                    puntos_este_mes = max(0, puntos_este_mes - 1)

                for _ in range(puntos_este_mes):
                    distancia = np.random.uniform(5000, 10000)
                    grado = np.random.uniform(0, 360)
                    varianza_lat = distancia * cos(grado) / 111320
                    varianza_lon = distancia * sin(grado) / (111320 * cos(fila['lat_mean']))
                    lat = varianza_lat + fila['lat_mean']
                    lon = varianza_lon + fila['lon_mean']

                    if filtros_no_sinteticos.puntoValido(lat, lon, parquetAnio, src, transformer):
                        listaLat.append(lat)
                        listaLon.append(lon)
                        fechas.append(filtros_no_sinteticos.crearFecha(dia_base, mes, anio_incendio))
                    else:
                        numNoIncendios_restante += 1

    return numNoIncendios_restante, listaLat, listaLon, fechas


def crearSinteticos(parquetAnio, src):

    '''
    Función para crear puntos sintéticos de no incendio, distribuidos proporcionalmente al número de incendios y al área de cada zona

    Parámetros:
    - parquetAnio: ruta al archivo parquet con los incendios del año
    - src: objeto raster con la información de la imagen

    Devuelve:
    - DataFrame con los puntos sintéticos generados
    '''
    
    load_dotenv()

    # Semilla para tener todos el mismo valor
    np.random.seed(42)

    # 1.- Leer incendios del año (una sola vez)
    df_incendios = pd.read_parquet(parquetAnio)  # df_incendios es el DataFrame completo

    #print(df_incendios)
    no_incendiosTotales = len(df_incendios) * 30
    incendiosTotales = len(df_incendios)

    # 2.- Definir máscaras de regiones
    mascarasRegiones = [
        'grupo3/BiogeoRegiones/AtlanticRegion.parquet', 'grupo3/BiogeoRegiones/BorealRegion.parquet', 'grupo3/BiogeoRegiones/MediterraneanRegion.parquet',
        'grupo3/BiogeoRegiones/BlackSeaRegion.parquet', 'grupo3/BiogeoRegiones/ContinentalRegion.parquet', 'grupo3/BiogeoRegiones/MacaronesianRegion.parquet',
        'grupo3/BiogeoRegiones/PannonianRegion.parquet', 'grupo3/BiogeoRegiones/SteppicRegion.parquet', 'grupo3/BiogeoRegiones/AnatolianRegion.parquet',
        'grupo3/BiogeoRegiones/ArcticRegion.parquet', 'grupo3/BiogeoRegiones/AlpineRegion.parquet'
    ]

    cliente = minioFunctions.crear_cliente()

    # 3.- Obtener DataFrames de incendios por zona (ya no son rutas, son DataFrames)
    listaZonas = filtros_no_sinteticos.filtrarZona(mascarasRegiones, df_incendios,cliente)

    mascaraRegionesGDF = []

    # 4.- Calcular áreas, número de incendios y FRP total por zona
    for i in range(len(listaZonas)):
        mascaraRegionesGDF.append(minioFunctions.bajar_fichero(cliente, mascarasRegiones[i], "gdf"))
    
    listaAreas = []
    listaIncendios = []
    listaFrpTotal = []
    areaTotal = 0

    for i, zona_df in enumerate(listaZonas):
        listaIncendios.append(len(zona_df))
        listaFrpTotal.append(zona_df['frp_mean'].sum())

        # Leer la máscara geográfica para calcular el área
        area = mascaraRegionesGDF[i].to_crs("EPSG:3035").geometry.area.sum() / 1e6  # km²
        areaTotal += area
        listaAreas.append(area)

    # 5.- Distribuir puntos de no incendio por zona
    alpha = 0.5
    listaNoIncendios = []
    for i in range(len(mascarasRegiones)):
        peso_incendios = listaIncendios[i] / incendiosTotales if incendiosTotales > 0 else 0
        peso_area = listaAreas[i] / areaTotal if areaTotal > 0 else 0
        no_incendios_zona = (alpha * peso_incendios + (1 - alpha) * peso_area) * no_incendiosTotales
        listaNoIncendios.append(round(no_incendios_zona))

    # 6.- Generar puntos sintéticos
    todas_lats = []
    todas_lons = []
    todas_fechas = []

    minio_config = {
        "AWS_S3_ENDPOINT": "minio.fdi.ucm.es",
        "AWS_HTTPS": "YES",
        "AWS_VIRTUAL_HOSTING": "FALSE",
        "GDAL_HTTP_UNSAFESSL": "YES",
    }

    ak, sk = minioFunctions.importar_keys()

    with rasterio.Env(**minio_config, aws_access_key_id=ak, aws_secret_access_key=sk):
        with rasterio.open("/vsis3/pd1/grupo3/mapa/mapa.tif") as src:
          transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)

          for i in range(len(mascarasRegiones)):
              # Puntos cercanos a incendios (solo si hay incendios en la zona)
              if listaIncendios[i] > 0 and listaFrpTotal[i] > 0:
                  restante, lats, lons, fechas = crearCercanos(
                      listaZonas[i],           # DataFrame de incendios de la zona
                      listaNoIncendios[i],     # total de puntos para esta zona
                      listaFrpTotal[i],        # suma de FRP en la zona
                      df_incendios,            # DataFrame completo (para validar)
                      src, transformer
                  )
                  todas_lats.extend(lats)
                  todas_lons.extend(lons)
                  todas_fechas.extend(fechas)
              else:
                  restante = listaNoIncendios[i]  # si no hay incendios, todos son aleatorios

              # Puntos aleatorios en la zona con los restantes
              if restante > 0:
                  anio = pd.to_datetime(df_incendios['date_first'].iloc[0]).year
                  lats_rand, lons_rand, fechas_rand = crearAleatorios(
                      mascarasRegiones[i],   # lista con una sola máscara
                      df_incendios,             # DataFrame completo (para validar)
                      restante,
                      anio,
                      src, transformer
                  )
                  todas_lats.extend(lats_rand)
                  todas_lons.extend(lons_rand)
                  todas_fechas.extend(fechas_rand)

    final_df = pd.DataFrame({'lat': todas_lats, 'lon': todas_lons, 'date': todas_fechas})

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/No_incendios/")
    # 7.- Devolver DataFrame final
    return final_df