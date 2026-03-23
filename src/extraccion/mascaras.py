import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point, box
from . import minioFunctions, parquet
import pandas as pd

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
    cliente = minioFunctions.crear_cliente()
    path = path_minio
    ficheros = cliente.list_objects("pd1", prefix=path, recursive=True) 

    for fichero in ficheros:
        if not fichero.is_dir: #Es un archivo y no una carpeta
            nombre = fichero.object_name.split("/")[-1]
            minioFunctions.bajar_fichero_local(cliente, f"{path_minio}/{nombre}", f"{path_destino}/{nombre}")     
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
    cliente = minioFunctions.crear_cliente()
    #minio_a_local(carpeta_local = "BiogeoRegiones_raw", path_minio = "grupo3/raw/Biogeoregiones")
    
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

def extraer_pais(pais = None):
    '''
    Extracción automática de la máscara del país o países pasados por parámetro desde MinIO
    
    :param pais: nombre del país a extraer 
    :param paises: lista de países a extraer
    :return mascara_pais: mascara del país o países (unificados) tipo GeoDataFrame
    '''
    assert pais is not None, "No hay ningún país para extraer"
    assert isinstance(pais, str) or isinstance(pais, list), "El país debe ser un string o una lista de strings"

    minio_a_local(carpeta_local = "Countries", path_minio = "grupo3/maps/Countries")

    #Extracción de los archivos desde local
    actual_p = Path(__file__).resolve()
    data = actual_p.parent.parent.parent / "data" / "Countries"
    path = data / "countries.shp"
    print(path)
    mundo = gpd.read_file(path)
    assert not mundo.empty, "Archivo vacio"

    #Para depurar
    #print("Columnas: ", list(mundo.columns))
    #print(mundo["NAME_ENGL"].unique())
    
    mundo = mundo.to_crs(epsg=4326) #El archivo lo descargué en epsg=3035
    if isinstance(pais, str): #Solo procesamos un país
        pais = mundo[mundo['NAME_ENGL'] == pais] 
    else: #Procesamos varios países
        pais = mundo[mundo['NAME_ENGL'].isin(pais)]

    #Conversión a GeoDataframe (make_valid() para arreglar errores de geometría rusos)
    mascara = pais['geometry'].make_valid().union_all() 
    mascara_pais = gpd.GeoDataFrame(geometry=[mascara], crs=pais.crs)

    return mascara_pais 

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
        parquet.to_parquet(valor["geometry"], nombre, "BiogeoRegiones") 

        
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
    '''
    Comprueba si el punto está dentro de la máscara
    :param mascara: GeoDataFrame con la geometría de la máscara
    :param punto: tipo Point con las coordenadas del punto a comprobar
    :return: True si el punto está dentro de la máscara
    '''
    assert not mascara.empty and not mascara is None, "No existe contenido en el GeoDataFrame"
    assert isinstance(punto, Point), "El punto no es del tipo Point"
    return mascara.iloc[0].geometry.contains(punto)

from shapely.geometry import box, Polygon
import matplotlib.pyplot as plt

def extraer_mascaras_faltantes():
    '''
    Función para extraer las máscaras de los países que faltan por generar
    puntos de no incendios. Se suben los parquets automáticamente a MinIO
    '''
    for pais in ["Belarus", "Spain", "Russian Federation", "Ukraine"]:
        df_pais = extraer_pais(pais)
        cliente = minioFunctions.crear_cliente()

        if pais == "Spain":
            # Seleccionamos solo Ceuta y Melilla con un box delimitador
            ceuta_melilla = box(-6.0, 35.0, -2.5, 35.5)
            df_pais_cm = df_pais.clip(ceuta_melilla)
            
            # Subimos y saltamos al siguiente país
            minioFunctions.subir_fichero(cliente, "grupo3/raw/Countries/mascara_Ceuta_Melilla.parquet", df_pais_cm)
            continue 

        elif pais == "Russian Federation":
            # ---------------------------------------------------------
            # 1. ZONA OESTE AISLADA (Kaliningrado) - INTACTA
            # ---------------------------------------------------------
            coords_spb = [
                (19.8, 54.4), (19.8, 55.0), (21.5, 55.3),
                (23.8, 55.3), (23.8, 54.4), (21.5, 54.4)
            ]
            poligono_spb = Polygon(coords_spb)
            df_spb = df_pais.clip(poligono_spb)
            
            # Subimos la máscara de Kaliningrado
            minioFunctions.subir_fichero(cliente, "grupo3/raw/Countries/mascara_San_Petersburgo.parquet", df_spb)

            # ---------------------------------------------------------
            # 2. ZONA RUSIA EUROPEA (Ajustada a la frontera real y corte en 35)
            # ---------------------------------------------------------
            # Empezamos en la Longitud 27 (fuera del país) para que el clip()
            # dibuje la frontera real automáticamente por la izquierda.
            coords_moscu = [
                (27.0, 50.0), # Suroeste (Fuera del país)
                (27.0, 68.0), # Noroeste (Fuera del país)
                (35.0, 68.0), # Noreste (Tope recto en Lon 35)
                (35.0, 50.0)  # Sureste (Tope recto en Lon 35)
            ]
            poligono_moscu = Polygon(coords_moscu)
            df_moscu = df_pais.clip(poligono_moscu)
            
            # Subimos la máscara de Rusia Europea
            minioFunctions.subir_fichero(cliente, "grupo3/raw/Countries/mascara_zona_Moscu.parquet", df_moscu)
            
            continue

        # Subimos a MinIO para Belarus, Ukraine u otros países normales
        nombre_archivo = f"grupo3/raw/Countries/mascara_{pais.replace(' ', '_')}.parquet"
        minioFunctions.subir_fichero(cliente, nombre_archivo, df_pais)


if __name__ == '__main__':
    # 1. EJECUCIÓN REAL: Genera los recortes y los sube a MinIO
    print("🚀 Iniciando proceso de extracción y subida a MinIO...")
    try:
        extraer_mascaras_faltantes()
        print("✅ ¡Proceso completado con éxito! Los archivos ya deberían estar en el bucket.")
    except Exception as e:
        print(f"❌ Error durante la subida: {e}")

    # 2. VERIFICACIÓN VISUAL (Opcional): 
    # Mantenemos esto solo para que estés 100% seguro de lo que acabas de subir
    print("\nGenerando vista previa de seguridad...")
    df_rusia = extraer_pais("Russian Federation")
    
    # Geometría Kaliningrado (Azul)
    poly_spb = Polygon([(19.8, 54.4), (19.8, 55.0), (21.5, 55.3), (23.8, 55.3), (23.8, 54.4), (21.5, 54.4)])
    # Geometría Rusia Europea (Rojo)
    poly_moscu = Polygon([(27.0, 50.0), (27.0, 68.0), (35.0, 68.0), (35.0, 50.0)])

    fig, ax = plt.subplots(figsize=(10, 8))
    df_rusia.plot(ax=ax, color='lightgrey', alpha=0.5)
    df_rusia.clip(poly_spb).plot(ax=ax, color='blue', label='Subido como SPB (Kaliningrado)')
    df_rusia.clip(poly_moscu).plot(ax=ax, color='red', label='Subido como Moscú (Rusia Europea)')
    
    ax.set_xlim(18, 40)
    ax.set_ylim(50, 67)
    plt.legend()
    plt.show()