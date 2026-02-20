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
    load_dotenv()

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    return access_key, secret_key

def crear_cliente():
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
    Solo admite los tipos de dataframe (df) y geodataframe (gdf). Especificamos el tipo por el parámetro type
    y por defecto se tomará el archivo como dataframe. 
    
    :param cliente: cliente de MinIO
    :param path_server: path del archivo en MinIO
    :param type: "df" o "gdf"
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

def bajar_carpeta(cliente, path_server: Path):
    ficheros = cliente.list_objects("pd1", prefix=path_server, recursive=True) 

    for fichero in ficheros:
        fichero.object_name
        
'''
#Subir a MinIO Biogeoregiones
cliente = crear_cliente()
path = Path(__file__).resolve().parent.parent.parent / "data" / "BiogeoRegions"
for archivo in path.iterdir():
    if archivo.is_file():
        destination_path = f"grupo3/raw/Biogeoregiones/{archivo.name}"
        subir_fichero(cliente, archivo, destination_path)
        print(f"Subido {archivo.name}")
'''

''' EJEMPLO SUBIR FICHERO
cliente = crear_cliente()
df = pd.DataFrame({"Esto" : ["es una prueba"]})
subir_fichero(cliente, "grupo3/prueba_subir_fichero.parquet", df)
'''
