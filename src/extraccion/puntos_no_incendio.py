import pandas as pd
from pyproj import Transformer
import numpy as np
import rasterio
from dotenv import load_dotenv
from . import minioFunctions
from . import filtros_no_incendio
from shapely.geometry import Point
import geopandas as gpd
from scipy.spatial.distance import cdist
from math import cos, sin, radians
import numpy as np
import pandas as pd

# Funciones encargadas de crear puntos sintéticos de acuerdo con el Área, el frp de fuego y el número de incendios
def crearAleatorios(mascara, df, noIncendios, anio, src, transformer):

    '''
    Crea puntos aleatorios dentro de una máscara geográfica, asegurando que sean válidos según ciertos filtros.
    
    :param mascara: ruta al archivo de la máscara geográfica (archivo Parquet con geometría)
    :param df: DataFrame con los incendios del año (para validar puntos)
    :param noIncendios: número total de puntos de no incendio a generar en esta zona (debe ser múltiplo de 12)
    :param anio: año para asignar a los puntos generados
    :param src: objeto raster para validar puntos
    :param transformer: objeto Transformer para validar puntos
    :return tuple: tres listas: latitudes, longitudes y fechas de los puntos generados
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
    
    #Generamos los puntos
    geometria_real = mascara_gdf.geometry.union_all()
    puntos_geometria = gpd.GeoSeries([geometria_real]).sample_points(noIncendios)
    puntos = list(puntos_geometria.iloc[0].geoms)

    # Generar puntos para cada mes (de 1 a 12)
    i = 0
    for mes in range(1, 13):
        for _ in range(puntos_por_mes):
            
            #Tres intentos de conseguir un mejor punto
            intentos = 0

            punto_actual = puntos.pop()
            if not punto_actual.within(geometria_real):
                continue

            lat = punto_actual.y
            lon = punto_actual.x
            
            while intentos < 3:
                intentos += 1

                if filtros_no_incendio.puntoValido(lat, lon, df, src, transformer):
                    listaLat.append(lat)
                    listaLon.append(lon)
                    dia = np.random.randint(1, 29)  # día aleatorio entre 1 y 28 (incluido)
                    fechas.append(filtros_no_incendio.crearFecha(dia, mes, anio))
                    break  
                else:
                    puntos_geometria = gpd.GeoSeries([geometria_real]).sample_points(1)
                    punto = list(puntos_geometria.iloc[0].geoms)
                    if not punto[0].within(geometria_real):
                        break
                    lat = punto[0].y
                    lon = punto[0].x

    return listaLat, listaLon, fechas


def crearCercanos(incendiosZona, numNoIncendios, frpTotal, df_completo, src, transformer):
    
    '''
    Crea puntos cercanos dentro de una máscara geográfica, asegurando que sean válidos según ciertos filtros.
    
    :param incendiosZona: dataFrame con los incendios de la zona donde se pretende crear puntos de no incendio
    :param numNoIncendios: el número de no incendios que se pretende crear en dicha zona
    :param frpTotal: frp de la zona
    :param df_completo: dataFrame con todos los datos de incendio 
    :param src: archivo raster del .tif de vegetación
    :param transformer: transformador de coordenadas geográficas
    :return tuple: número de no incendios restantes (no creados, para aleatorios), latitudes, longitudes y fechas de los puntos generados
    '''

    listaLat = []
    listaLon = []
    fechas = []

    if len(incendiosZona) == 0 or frpTotal == 0:
        return numNoIncendios, listaLat, listaLon, fechas

    numNoIncendios_restante = numNoIncendios

    for i in range(len(incendiosZona)):
        fila = incendiosZona.iloc[i]
        importancia = fila['frp_mean'] / frpTotal
        numPuntos = round(importancia * numNoIncendios)

        if numPuntos > 0:
            numNoIncendios_restante -= numPuntos
            puntos_por_mes = numPuntos // 12
            resto = numPuntos % 12

            fecha_incendio = pd.to_datetime(fila['date'])
            mes_incendio = fecha_incendio.month
            anio_incendio = fecha_incendio.year
            dia_base = min(fecha_incendio.day, 28)

            for mes in range(1, 13):
                # Repartimos el resto entre los primeros meses
                puntos_este_mes = puntos_por_mes + (1 if mes <= resto else 0)

                # Si es el mes del incendio, intentamos no poner puntos ahí o poner uno menos
                if mes == mes_incendio:
                    puntos_este_mes = max(0, puntos_este_mes - 1)

                for _ in range(puntos_este_mes):
                    distancia = np.random.uniform(5000, 10000) # Metros
                    # Convertir grado a radianes para sin/cos
                    angulo_rad = radians(np.random.uniform(0, 360))
                    
                    # Corrección de distancia en grados (Aproximación Haversine simplificada)
                    varianza_lat = (distancia * cos(angulo_rad)) / 111320
                    # IMPORTANTE: El coseno de la latitud también debe estar en radianes
                    cos_lat = cos(radians(fila['lat']))
                    varianza_lon = (distancia * sin(angulo_rad)) / (111320 * cos_lat)
                    
                    lat = varianza_lat + fila['lat']
                    lon = varianza_lon + fila['lon']

                    # Usamos df_completo para validar, no solo los de la zona
                    if filtros_no_incendio.puntoValido(lat, lon, df_completo, src, transformer):
                        listaLat.append(lat)
                        listaLon.append(lon)
                        fechas.append(filtros_no_incendio.crearFecha(dia_base, mes, anio_incendio))
                    else:
                        # Si el punto cae en agua o sobre un incendio real, se devuelve al saco de aleatorios
                        numNoIncendios_restante += 1

    return numNoIncendios_restante, listaLat, listaLon, fechas



def crearSinteticos(df_incendios, subir = True):

    '''
    Función para crear puntos sintéticos de no incendio, distribuidos proporcionalmente al número de incendios y al área de cada zona
    
    :param df_incendios: DataFrame con los incendios del año
    :param subir: booleano que indica si se debe subir el resultado a MinIO
    :return pd.DataFrame: DataFrame con los puntos sintéticos generados
    '''
    
    load_dotenv()

    # Semilla para tener todos el mismo valor
    np.random.seed(42)

    # 1.- Objetener límites de creación 

    no_incendiosTotales = len(df_incendios) * 30
    incendiosTotales = len(df_incendios)

    # 2.- Definir máscaras de regiones
    cliente = minioFunctions.crear_cliente()
    mascarasRegiones = minioFunctions.listar_bucket(cliente, "grupo3/raw/Biogeoregiones/")
    mascarasRegiones += [
        'grupo3/raw/Countries/mascara_zona_Moscu.parquet',
        'grupo3/raw/Countries/mascara_San_Petersburgo.parquet',
        'grupo3/raw/Countries/mascara_Belarus.parquet',
        'grupo3/raw/Countries/mascara_Norte_Africa.parquet'
    ]

    # 3.- Obtener DataFrames de incendios por zona (ya no son rutas, son DataFrames)
    listaZonas = filtros_no_incendio.filtrarZona(mascarasRegiones, df_incendios,cliente)

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
        with rasterio.open("/vsis3/pd1/grupo3/maps/mapa/mapa.tif") as src:
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
                  anio = pd.to_datetime(df_incendios['date'].iloc[0]).year
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
    
    print("Hecho")
    
    if subir:
        minioFunctions.preguntar_subida(final_df, "grupo3/raw/No_incendios/")
    # 7.- Devolver DataFrame final
    return final_df


def contarSinteticosPorArea(df_incendios):
    '''
    Calcula y devuelve ÚNICAMENTE el número de puntos sintéticos que se generarían por cada zona,
    sin llegar a generarlos físicamente. Ideal para testeos rápidos.
    
    :param df_incendios: DataFrame con los incendios del año
    :return list: Lista de strings con el número de puntos calculados por zona
    '''
    load_dotenv()
    np.random.seed(42)

    # 1.- Obtener límites de creación
    no_incendiosTotales = len(df_incendios) * 30
    incendiosTotales = len(df_incendios)

    # 2.- Definir máscaras de regiones
    mascarasRegiones = [
        'grupo3/raw/Biogeoregiones/AtlanticRegion.parquet', 'grupo3/raw/Biogeoregiones/BorealRegion.parquet', 'grupo3/raw/Biogeoregiones/MediterraneanRegion.parquet',
        'grupo3/raw/Biogeoregiones/BlackSeaRegion.parquet', 'grupo3/raw/Biogeoregiones/ContinentalRegion.parquet', 'grupo3/raw/Biogeoregiones/MacaronesianRegion.parquet',
        'grupo3/raw/Biogeoregiones/PannonianRegion.parquet', 'grupo3/raw/Biogeoregiones/SteppicRegion.parquet', 'grupo3/raw/Biogeoregiones/AnatolianRegion.parquet',
        'grupo3/raw/Biogeoregiones/ArcticRegion.parquet', 'grupo3/raw/Biogeoregiones/AlpineRegion.parquet','grupo3/raw/Countries/mascara_Belarus.parquet', 'grupo3/raw/Countries/mascara_Norte_Africa.parquet',
        'grupo3/raw/Countries/mascara_zona_Moscu.parquet', 'grupo3/raw/Countries/mascara_San_Petersburgo.parquet']

    cliente = minioFunctions.crear_cliente()

    # 3.- Obtener DataFrames de incendios por zona 
    listaZonas = filtros_no_incendio.filtrarZona(mascarasRegiones, df_incendios, cliente)

    mascaraRegionesGDF = []

    # 4.- Calcular áreas y número de incendios por zona
    for i in range(len(listaZonas)):
        mascaraRegionesGDF.append(minioFunctions.bajar_fichero(cliente, mascarasRegiones[i], "gdf"))

    listaAreas = []
    listaIncendios = []
    areaTotal = 0

    for i, zona_df in enumerate(listaZonas):
        listaIncendios.append(len(zona_df))

        # Leer la máscara geográfica para calcular el área (¡Ahora dentro del bucle!)
        area = mascaraRegionesGDF[i].to_crs("EPSG:3035").geometry.area.sum() / 1e6  # km²
        areaTotal += area
        listaAreas.append(area)

    # 5.- Distribuir puntos de no incendio por zona
    alpha = 0.5
    resultados = []

    for i in range(len(mascarasRegiones)):
        peso_incendios = listaIncendios[i] / incendiosTotales if incendiosTotales > 0 else 0
        peso_area = listaAreas[i] / areaTotal if areaTotal > 0 else 0
        no_incendios_zona = (alpha * peso_incendios + (1 - alpha) * peso_area) * no_incendiosTotales

        puntos_redondeados = round(no_incendios_zona)
        nombre_limpio = mascarasRegiones[i].split('/')[-1].replace('.parquet', '')

        resultados.append(f"{nombre_limpio}: {puntos_redondeados}")

    # 6.- Imprimir resultados
    for linea in resultados:
        print(linea)

    return resultados


def crearSinteticosUnaZona(df_incendios, mascara, num_puntos, subir = True):

    '''
    Función para crear puntos sintéticos de no incendio para una única zona específica.

    :param df_incendios: DataFrame con los incendios del año
    :param mascara: ruta al archivo parquet de la máscara
    :param num_puntos: cantidad de puntos sintéticos a generar
    :param subir: booleano para subir o no el resultado a MinIO
    :return pd.DataFrame: DataFrame con los puntos sintéticos generados
    '''
    
    load_dotenv()

    # Semilla para tener todos el mismo valor
    np.random.seed(42)

    cliente = minioFunctions.crear_cliente()

    # 1.- Obtener DataFrames de incendios por zona
    listaZonas = filtros_no_incendio.filtrarZona([mascara], df_incendios, cliente)

    zona_df = listaZonas[0]
    frp_total = zona_df['frp_mean'].sum() if len(zona_df) > 0 else 0

    # 2.- Generar puntos sintéticos
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
        with rasterio.open("/vsis3/pd1/grupo3/maps/mapa/mapa.tif") as src:
            transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)

            # Puntos cercanos a incendios (solo si hay incendios en la zona)
            if len(zona_df) > 0 and frp_total > 0:
                restante, lats, lons, fechas = crearCercanos(
                    zona_df,                  # DataFrame de incendios de la zona
                    num_puntos,               # total de puntos para esta zona
                    frp_total,                # suma de FRP en la zona
                    df_incendios,             # DataFrame completo (para validar)
                    src, transformer
                )
                todas_lats.extend(lats)
                todas_lons.extend(lons)
                todas_fechas.extend(fechas)
            else:
                restante = num_puntos  # si no hay incendios, todos son aleatorios

            # Puntos aleatorios en la zona con los restantes
            if restante > 0:
                anio = pd.to_datetime(df_incendios['date'].iloc[0]).year
                lats_rand, lons_rand, fechas_rand = crearAleatorios(
                    mascara,                  # ruta de la máscara
                    df_incendios,             # DataFrame completo (para validar)
                    restante,
                    anio,
                    src, transformer
                )
                todas_lats.extend(lats_rand)
                todas_lons.extend(lons_rand)
                todas_fechas.extend(fechas_rand)

    final_df = pd.DataFrame({'lat': todas_lats, 'lon': todas_lons, 'date': todas_fechas})
    
    print("Hecho")
    
    print(len(final_df))

    if subir:
        minioFunctions.preguntar_subida(final_df, "grupo3/raw/No_incendios/Nuevas_Zonas/")
        
    # 3.- Devolver DataFrame final
    return final_df


def puntosParaBorrar(df, ruta_mascara, puntos, cliente):

    '''
    Función para eliminar puntos del dataframe df, pertenecientes a una zona. No se eliminan todos, solo una parte

    :param df: DataFrame con todos los puntos, del que se quieren eliminar
    :param ruta_mascara: ruta de la mascara que cubre la zona correspondiente
    :param puntos: número de puntos que se pretende eliminar
    :param cliente: cliente MinIO
    :return pd.DataFrame: DataFrame con los puntos que nos queremos quedar
    '''

    # Filtrado por máscara
    df_zona = filtros_no_incendio.filtrar_zona_eliminar(ruta_mascara, df, cliente)
    
    if df_zona.empty:
        return pd.DataFrame(columns=['lat', 'lon', 'date'])

    # Preparación Temporal
    df_zona['date'] = pd.to_datetime(df_zona['date'])
    
    # Columna combinada Año-Mes 
    df_zona['periodo'] = df_zona['date'].dt.to_period('M')
    
    # Mezcla aleatoria 
    df_zona = df_zona.sample(frac=1, random_state=42).reset_index(drop=True)

    puntos_seleccionados = []
    # Umbral de 10km en grados
    distancia_min = 10 / 111.32 

    # Obtenemos todos los meses-años (para poder repartir equitativamente)
    periodos_disponibles = sorted(df_zona['periodo'].unique())
    
    # Usamos un set para llevar control de los índices usados
    indices_usados = set()
    
    id = 0
    sin_exito = 0 
    num_max_fallos = len(df_zona) 

    while len(puntos_seleccionados) < puntos and sin_exito < num_max_fallos:
        periodo_actual = periodos_disponibles[id % len(periodos_disponibles)]
        
        candidatos = df_zona[(df_zona['periodo'] == periodo_actual) & (~df_zona.index.isin(indices_usados))]
        
        if not candidatos.empty:
            fila = candidatos.iloc[0]
            id_ini = candidatos.index[0]
            
            punto_propuesto = (fila['lat'], fila['lon'])
            fecha_propuesta = fila['date']
            
            # 5. 10km
            es_valido = True
            if puntos_seleccionados:
                # Comprobar Distancia
                coords_existentes = [(p['lat'], p['lon']) for p in puntos_seleccionados]
                distancias = cdist([punto_propuesto], coords_existentes, metric='euclidean')[0]
                
                # Comprobar Fecha (Evitar duplicados exactos)
                fechas_existentes = [p['date'] for p in puntos_seleccionados]
                
                if np.any(distancias < distancia_min) or (fecha_propuesta in fechas_existentes):
                    es_valido = False
            
            if es_valido:
                puntos_seleccionados.append({
                    'lat': fila['lat'],
                    'lon': fila['lon'],
                    'date': fila['date']
                })
                sin_exito = 0 # Reseteamos si tuvimos éxito
            else:
                sin_exito += 1
            
            # Marcamos como usado para no volver a evaluar el punto
            indices_usados.add(id_ini)
        else:
            # Si este mes ya no tiene más puntos pasamos al siguiente
            sin_exito += 1

        id += 1

    final_df = pd.DataFrame(puntos_seleccionados)
    
    if not final_df.empty:
        resumen_años = final_df['date'].dt.year.value_counts().sort_index()
        print("Puntos seleccionados por año:")
        print(resumen_años)
        
    return final_df


def eliminarPuntosSeleccionados(df_grande, df_pequeno):
    '''
    Función para eliminar puntos del dataframe grande, que coinciden con el pequeño en lat, lon y date.

    :param df_grande: DataFrame con todos los puntos
    :param df_pequeno: DataFrame con los puntos que se quieren eliminar
    :return pd.DataFrame: DataFrame con los puntos que nos queremos quedar
    '''

    cols = ['lat', 'lon', 'date']

    df_grande['date'] = pd.to_datetime(df_grande['date'])
    df_pequeno['date'] = pd.to_datetime(df_pequeno['date'])
    
    df_temp = df_grande.merge(df_pequeno[cols], on=cols, how='left', indicator=True)
    df_final = df_temp[df_temp['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    return df_final.reset_index(drop=True)


def eliminarZona(ruta_mascara, parquet, cliente):
    '''
    Función para eliminar puntos de incendio pertenecientes a un parquet, de una zona determinada.

    :param ruta_mascara: ruta al archivo parquet de la máscara
    :param parquet: DataFrame con los incendios
    :param cliente: cliente MinIO
    :return pd.DataFrame: DataFrame con los puntos de no incendio que no pertenecen a esa zona
    '''

    eliminar = filtros_no_incendio.filtrar_zona_eliminar(ruta_mascara, parquet, cliente)
    lats = eliminar['lat'].to_list()
    lons = eliminar['lon'].to_list()
    df_filtrado = parquet[~((parquet["lat"].isin(lats)) & (parquet["lon"].isin(lons)))]

    return df_filtrado