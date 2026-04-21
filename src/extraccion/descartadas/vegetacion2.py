import rasterio
import pandas as pd
from pyproj import Transformer
import time
from rasterio.windows import Window
from dotenv import load_dotenv
import asyncio
from extraccion import minioFunctions
from extraccion import interrupcion
import numpy as np

def obtenerNumero(lat, lon, src, transformer):
    '''
    Extrae el valor del pixel correspondiente a una coordenada dentro de un archivo raster
    
    Casos limite:
    - Si no hay nada que leer devuelve -1
    - Cualquier valor negativo se reasigna al indice 44

    
    :param lat: Latitud
    :param lon: Longitud
    :param src: Archivo raster
    :param transformer: Transformador de coordenadas
    :return float: Valor del pixel
    '''
    x, y = transformer.transform(lon, lat)
    row, col = src.index(x, y)
                
    window = Window(col, row, 1, 1)
    data = src.read(1, window=window)
            
    if data.size > 0:
        num = data[0, 0]
        if num < 0:
            vecinos = Window(col - 3, row - 3, 5, 5)
            data_vecinos = src.read(1, window=vecinos)

            if data_vecinos.size > 0:
                vecinos_clean = np.where((data_vecinos == src.nodata) | (data_vecinos < 0), np.nan, data_vecinos)
                if not np.isnan(vecinos_clean).all():
                    media_vecinos = np.nanmean(vecinos_clean)
                    return float(media_vecinos)
    else:
        vecinos = Window(col - 1, row - 1, 3, 3)
        data_vecinos = src.read(1, window=vecinos)
      
    return num

def lista_entorno(lista_puntos, df_vegetacion): 
    """
    Mapea una lista de coordenadas a sus respectivas categorias de vegetacion o terreno
    
    Abre una conexion al raster alojado en MinIO y traduce el valor numerico de cada 
    pixel utilizando el indice de df_vegetacion
    
    Importante:
    - Las descriptivas estan en 'Column6'
    - Solo procesa valores de pixel en el rango [0, 44] y los valores fuera de este
      rango se clasifican como 'Sin datos'

    
    :params lista_puntos: Lista de tuplas
    :params df_vegetacion: DataFrame .
    :return list: Tipo de vegetación o sin datos.
    """
    load_dotenv()
    
    minio_config = {
        "AWS_S3_ENDPOINT": "minio.fdi.ucm.es",
        "AWS_HTTPS": "YES",
        "AWS_VIRTUAL_HOSTING": "FALSE",
        "GDAL_HTTP_UNSAFESSL": "YES",
    }
    ak, sk = minioFunctions.importar_keys()

    with rasterio.Env(**minio_config,
                     aws_access_key_id=ak,
                     aws_secret_access_key=sk):
        
        with rasterio.open("/vsis3/pd1/grupo3/mapa/mapa.tif") as src:
            print(f"Número total de bandas: {src.count}")
            print(f"Índices de las bandas: {src.indexes}")
            print(f"Limites del mapa .tif: {src.bounds}")
            print(f"Sistema de coordenadas usado: {src.crs}")

            transformer = Transformer.from_crs("EPSG:4326", src.crs, always_xy=True)
            lista_vegetacion = []
            
            for i, (lon, lat) in enumerate(lista_puntos):
                num = obtenerNumero(lat, lon, src, transformer)
                if num > -1 and num < 45:
                    lista_vegetacion.append(df_vegetacion.loc[num]["Column6"])
                else:
                    lista_vegetacion.append("Sin datos")
                
                if i % 1000 == 0:
                    print(f"Dato {i} extraído")

            return lista_vegetacion

async def df_vegetacion2(fires, limit=20, fecha_ini=None, fecha_fin=None):
    """
    Se extraen los datos de vegetacion para una serie de incendios
    
    Requiere que el DataFrame fires contenga las columnas 'lat', 'lon' y 'date'

    :params fires: Dataframe con los puntos
    :params limit: Límite de filas
    :params fecha_ini: Fecha de inicio
    :params fecha_fin: Fecha de fin
    :return pd.DataFrame: DataFrame final
    """
    ini = time.time()
    ak, sk = minioFunctions.importar_keys()

    df_aux = pd.read_csv("s3://pd1/grupo3/mapa/mapa_vegetacion.csv", 
                            storage_options={
                                "key": ak,
                                "secret": sk,
                                "client_kwargs": {"endpoint_url": "https://minio.fdi.ucm.es", "verify": False}
                            })

    fin_none = fecha_fin is None
    ini_none = fecha_ini is None

    if not fin_none and not ini_none: 
        fires = fires[fires['date'].between(fecha_ini, fecha_fin)]

    if limit != -1:
        fires = fires.head(limit)   
    
    lista_puntos = list(zip(fires['lon'], fires['lat']))

    try:
        lista_res = await asyncio.to_thread(lista_entorno, lista_puntos, df_aux)
    except KeyboardInterrupt:
        print("\n Interrupción detectada. No hay datos parciales para guardar en vegetacion2 (proceso síncrono).")
        raise
    
    fires = fires[['lat','lon','date']].copy().reset_index(drop = True)

    final_df = pd.DataFrame(lista_res, columns=["vegetacion2"])
    final_df = pd.concat([final_df, fires], axis = 1)
    final_df = final_df.rename(columns={'lat':'lat', 'lon':'lon', 'date':'date'})

    print(f"Finalizado en {time.time() - ini:.2f}s")
    print(final_df.head(limit))

    minioFunctions.preguntar_subida(final_df, "grupo3/raw/Vegetacion2/")

    return final_df