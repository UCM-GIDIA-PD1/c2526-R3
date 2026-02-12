import geopandas as gpd
import geodatasets
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

#Para ver todo el contenido del df
#pd.set_option('display.max_columns', None)
#pd.set_option('display.max_colwidth', None)

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
    mascara_europa = gpd.GeoDataFrame(geometry=[mascara])

    return mascara_europa 


def extraer_biogeografica(path = None):
    '''
    if (path = None) Extracción automática desde src/extraccion/ => data/Countries/countries.shp
    :param path: ruta del archivo o vacío
    :return bio_mascaras: 12 máscaras con las bioregiones en Europa tipo GeoDataFrame
    '''

    #Extracción del archivo
    if (path == None):
        actual_p = Path.cwd()
        data = actual_p.parent.parent / "data" / "BiogeoRegions"
        path = data / "BiogeoRegions2016.shp"

    f = gpd.read_file(path)
    assert not f.empty, "Archivo vacio"

    print("Columnas: ", list(f.columns))
    print("Hay ", len(f.values), " regiones biogeograficas")

    #Conversión a GeoDataFrame
    bioregiones = f['name'].to_list()
    
    print("Nombre de las bioregiones: ", bioregiones)

    #Creamos una máscara para cada una
    mascaras = {}
    for region in bioregiones:
        geom = f.loc[f['name'] == region, "geometry"].union_all()
        mascara = gpd.GeoDataFrame(geometry=[geom])
        mascaras[region] = mascara

    return mascaras 


mascaras = extraer_biogeografica()
print(mascaras.keys())
mascaras[next(iter(mascaras))].plot(edgecolor="black")
plt.show()

