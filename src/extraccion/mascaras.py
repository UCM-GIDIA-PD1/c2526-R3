import io
import geopandas as gpd
import geodatasets
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Point
from shapely.prepared import prep
from minioFunctions import *

from parquet import to_parquet

'''
Las funciones a usar son:
parse_parquet(path): importa un archivo en .parquet y lo pasa a geodataframe
is_in(mascara, punto): comprueba si el punto esta dentro de la máscara

Las demás funciones han sido usadas para la creación de los .parquet
'''

def minio_a_local(carpeta_local, path_minio):
     #Guardaremos los archivos en local (al trabajar con distintos tipos de archivo)
    path = Path(__file__).resolve().parent.parent.parent / "data"
    if not path.exists(): #Creamos si no está creada data
        path.mkdir()

    path_destino = path / carpeta_local
    if not path_destino.exists():  #Creamos si no está creada
        path_destino.mkdir()

    #Traemos los archivos de MinIO a local 
    cliente = crear_cliente()
    path = path_minio
    ficheros = cliente.list_objects("pd1", prefix=path, recursive=True) 

    for fichero in ficheros:
        if not fichero.is_dir: #Es un archivo y no una carpeta
            nombre = fichero.object_name.split("/")[-1]
            bajar_fichero_local(cliente, f"{path_minio}/{nombre}", f"{path_destino}/{nombre}")     
            print(f"Subido {nombre}")

#El archivo es .shp (shapefile) que almacena datos vectoriales que almacena la forma y ubicación de puntos geográficos
def extraer_europa_raw(path = None):
    '''
    Extracción automática de la máscara de Europa desde MinIO
    
    :param path: ruta del archivo o vacío
    :return mascara_europa: mascara de europa tipo GeoDataFrame
    '''
    minio_a_local(carpeta_local = "Europa", path_minio = "grupo3/raw/Countries")

    #Extracción de los archivos desde local
    actual_p = Path(__file__).resolve()
    data = actual_p.parent.parent.parent / "data" / "Europa"
    path = data / "ne_50m_admin_0_countries.shp"
    print(path)
    mundo = gpd.read_file(path)
    assert not mundo.empty, "Archivo vacio"
    europa = mundo[mundo['CONTINENT'] == 'Europe'] 

    #Conversión a GeoDataframe
    mascara = europa['geometry'].union_all() #junta todos los polígonos correspondientes a cada país eliminando límites internos
    mascara_europa = gpd.GeoDataFrame(geometry=[mascara], crs=europa.crs)

    return mascara_europa 

def extraer_biogeografica_raw():
    '''
    Extracción automática de las máscaras de las biogeoregiones desde MinIO
    :return bio_mascaras: 12 máscaras con las bioregiones en Europa tipo GeoDataFrame
    '''
    cliente = crear_cliente()
    minio_a_local(carpeta_local = "BiogeoRegiones_raw", path_minio = "grupo3/raw/Biogeoregiones")
    
    #Extracción de los archivos desde local
    actual_p = Path(__file__).resolve()
    data = actual_p.parent.parent.parent / "data" / "BiogeoRegiones_raw"
    print(f"ruta: {data}")
    assert data.exists(), "La ruta de archivos (c2526-R3/data/BiogeoRegiones_raw/) no existe o no es correcta."
    path = data / "BiogeoRegions2016.shp"
    assert path.exists(), f"No existe el archivo BiogeoRegions2016.shp"
        
    f = gpd.read_file(path)
    assert not f.empty, "Archivo vacio"

    print("Columnas: ", list(f.columns))
    print("Hay ", len(f.values), " regiones biogeograficas")

    #Conversión a GeoDataFrame
    bioregiones = f['name'].to_list()

    #Creamos una máscara para cada una
    mascaras = {}
    for region in bioregiones:
        print(f"parseando {region}")
        geom = f.loc[f['name'] == region, "geometry"].union_all()
        mascara = gpd.GeoDataFrame(geometry=[geom], crs=f.crs)
        mascaras[region] = mascara

    return mascaras 

def bioregions_to_parquet(mascaras: dict):
    '''
    Guarda varios GeoDataFrames en distintos archivos
    :param mascaras: Diccionario
    :param nombre: nombre del archivo
    '''

    assert isinstance(mascaras, dict), "La variable no es un diccionario"
    for clave, valor in mascaras.items():
        assert "geometry" in valor.columns, "No existen datos geometricos"

        nombre = clave.replace("Bio-geographical", "").replace(" ", "").strip()
        to_parquet(valor["geometry"], nombre, "BiogeoRegiones") 

        


def parse_parquet(path: str):
    '''
    Convierte parquet a geodataframe
    :param path: ruta al archivo (string)
    '''
    gdf = gpd.read_parquet(path)

    #Comprobamos que es un GeoDataFrame
    assert isinstance(gdf, gpd.GeoDataFrame), f"El archivo no es un GeoDataFrame, es un: {type(gdf)}"
    gdf = gdf.to_crs(4326) #Es el sistema de coordenadas que utiliza Point() 
    
    return gdf

def is_in(mascara: gpd.GeoDataFrame, punto: Point):
    assert not mascara.empty and not mascara is None, "No existe contenido en el GeoDataFrame"
    assert Point != None, "Punto vacío"
    return mascara.iloc[0].geometry.contains(punto)


mascaras = extraer_europa_raw()