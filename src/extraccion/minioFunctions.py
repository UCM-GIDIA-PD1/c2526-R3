from minio import Minio
from pathlib import Path
import io
import pandas as pd
import geopandas as gpd
import os
from dotenv import load_dotenv

'''
Comprobar:
- Variables de entorno AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY en .env
con las claves de acceso a MinIO
- Tener VPN de la Complu activada.
'''

def importar_keys():
    '''
    Importa las claves desde el archivo .env
    '''
    load_dotenv()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    return access_key, secret_key

def crear_cliente():
    '''
    Crea el cliente de acceso a minio
    '''
    ak, sk = importar_keys()
    
    cliente = Minio(
        endpoint="minio.fdi.ucm.es",
        access_key = ak,
        secret_key = sk
    )

    return cliente
    
def subir_fichero(cliente, path_server: Path, df):
    assert isinstance(df, gpd.GeoDataFrame) or isinstance(df, pd.DataFrame), f"Ningún dataframe o geodataframe se pasó por parámetro"
    '''
    Sube un dataframe o geodataframe como parquet a la ruta especificada de minio.
    
    :param cliente: cliente de MinIO (función crear_cliente())
    :param path_server: path del archivo en MinIO
    :param df: DataFrame o GeoDataFrame
    '''
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    lenght = buffer.tell() #Obtenemos longitud de los datos
    buffer.seek(0) #Como el buffer es un puntero, hacemos que apunte al principio del fichero

    cliente.put_object(
        bucket_name= "pd1",
        object_name=path_server,
        data=buffer,
        length=lenght
    )
    print(f"Fichero subido como {path_server}")
    
def bajar_fichero(cliente, path_server: Path, type = "df"):
    '''
    Baja un dataframe o geodataframe como parquet desde la ruta especificada.

    :param cliente: cliente de MinIO (función crear_cliente())
    :param path_server: path del archivo en MinIO
    :param type: especificar el tipo "df" o "gdf"
    '''
    assert type == "gdf" or type == "df", f"Tipo especificado no válido: {type}, especifique 'df' o 'gdf'"
    
    response = None
    try:
        response = cliente.get_object(
            bucket_name="pd1",
            object_name=path_server,
        )

        buffer = io.BytesIO(response.read()) #Almacenamos fichero en buffer de memoria
        
        if (type == "gdf"):
            gdf = gpd.read_parquet(buffer)
            print(f"Geodataframe importado correctamente")
            return gdf
        else:
            df = pd.read_parquet(buffer)
            print(f"Dataframe importado correctamente")
            return df
    
    except Exception as e: 
        print(f"Error al conectar con el servidor: {e}")

    finally:
        if response: 
            response.close()

def bajar_fichero_local(cliente, path_server: Path, path_local: Path):
    cliente.fget_object(
        bucket_name="pd1",
        object_name=path_server,
        file_path=path_local,
    )

# Función encargada de automatizar la subida de .parquets a Minio

def preguntar_subida(df, ruta_carpeta = "grupo3/Datos/"):
    """
    Pregunta al usuario si desea subir un DataFrame/GeoDataFrame a MinIO.
    """
    nombre_sugerido="datos.parquet"

    print("\n Subir a MinIO ")
    respuesta = input("¿Desea subir este archivo a MinIO? (s/n): ").strip().lower()
    if respuesta != 's':
        print("Archivo no subido.")
        return False

    print(f"Ruta en el bucket: {ruta_carpeta}")

    nombre = input(f"Nombre del archivo para '{nombre_sugerido}': ").strip()
    
    if not nombre:
        nombre = nombre_sugerido
    if not nombre.endswith('.parquet'):
        nombre += '.parquet'

    ruta_completa = f"{ruta_carpeta}{nombre}"
    print(f"Ruta completa: {ruta_completa}")

    try:
        cliente = crear_cliente()
        subir_fichero(cliente, ruta_completa, df)
        print(f" Archivo subido correctamente a {ruta_completa}")
        return True
    except Exception as e:
        print(f" Error al subir el archivo: {e}")
        return False
    
    
def bajar_csv(cliente, path_server: Path, **kwargs):
    
    """
    Descarga un archivo CSV desde MinIO y lo devuelve como DataFrame.
    
    :param cliente: cliente de MinIO
    :param path_server: ruta del archivo CSV en MinIO
    :param **kwargs: argumentos 
    :return: DataFrame con los datos del CSV
    """

    response = None
    try:
        response = cliente.get_object(
            bucket_name="pd1",
            object_name=path_server,
        )

        buffer = io.BytesIO(response.read())  
        df = pd.read_csv(buffer, **kwargs)  
        
        print(f"CSV importado correctamente desde {path_server}")
        return df
    
    except Exception as e: 
        print(f"Error al conectar con el servidor: {e}")
        return None

    finally:
        if response: 
            response.close()