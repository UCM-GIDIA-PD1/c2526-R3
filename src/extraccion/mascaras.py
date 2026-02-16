import geopandas as gpd
import geodatasets
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Point
from shapely.prepared import prep

from parquet import to_parquet

#Para ver todo el contenido del df (no recomendado porque son muchas coordenadas)
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_colwidth', None)

'''
Las funciones a usar son:
parse_parquet(path): importa un archivo en .parquet y lo pasa a geodataframe
is_in(mascara, punto): comprueba si el punto esta dentro de la máscara

Las demás funciones han sido usadas para la creación de los .parquet
'''

#El archivo es .shp (shapefile) que almacena datos vectoriales que almacena la forma y ubicación de puntos geográficos
def mascara_europa(path = None):
    '''
    if (path = None) Extracción automática desde src/extraccion/ => data/Countries/countries.shp
    
    :param path: ruta del archivo o vacío
    :return mascara_europa: mascara de europa tipo GeoDataFrame
    '''
    #Extracción del archivo
    if (path == None):
        actual_p = Path.cwd()
        data = actual_p.parent.parent / "data" / "Countries"
        path = data / "countries.shp"

    mundo = gpd.read_file(path)
    assert not mundo.empty, "Archivo vacio"
    europa = mundo[mundo['CONTINENT'] == 'Europe'] 

    #Conversión a GeoDataframe
    mascara = europa['geometry'].union_all() #junta todos los polígonos correspondientes a cada país eliminando límites internos
    mascara_europa = gpd.GeoDataFrame(geometry=[mascara], crs=europa.crs)

    return mascara_europa 

def extraer_biogeografica(path = None):
    '''
    if (path = None) Extracción automática desde src/extraccion/ => data/BiogeoRegions/BiogeoRegions2016.shp
    :param path: ruta del archivo o vacío
    :return bio_mascaras: 12 máscaras con las bioregiones en Europa tipo GeoDataFrame
    '''
    #Extracción del archivo
    if path is None:
        actual_p = Path(__file__).resolve()
        data = actual_p.parent.parent.parent / "data" / "BiogeoRegions"
        print(f"ruta: {data}")
        assert data.exists(), "La ruta de archivos (c2526-R3/data/BiogeoRegions/) no existe o no es correcta."
        path = data / "BiogeoRegions2016.shp"
        assert path.exists(), "No existe el archivo BiogeoRegions2016.shp"

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
        print(f"Exportando {nombre}...")
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


mascaras = extraer_biogeografica()
bioregions_to_parquet(mascaras)
