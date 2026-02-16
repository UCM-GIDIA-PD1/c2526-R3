from pathlib import Path
import geopandas as gpd
import geodatasets

def to_parquet(mascara: gpd.GeoDataFrame, nombre: str, carpeta = None):
    '''
    Guarda un GeoDataFrame como parquet. 

    :param mascara: GeoDataFrame
    :param nombre: nombre del archivo
    :param carpeta: 
        Si carpeta == None => Se guarda dentro de "data/"
        Si carpeta != None => Se guarda dentro de "data/carpeta/" y si no existe "carpeta/" la crea dentro de "data/".
    '''
    proyecto = Path(__file__).resolve().parent.parent.parent #c2526-R3/src/extraccion => c2526-R3
    data_path = proyecto / "data"

    if not data_path.exists(): #Si no existe la carpeta data se crea
        print("Creando carpeta data en el proyecto...")
        data_path.mkdir()

    if not carpeta is None: #Archivo dentro de data/carpeta/
        data_path = data_path / carpeta
        if not data_path.exists(): #Si no existe la carpeta pasada como argumento se crea
            print(f"Creando carpeta {carpeta} en el proyecto...")
            data_path.mkdir()

    print(data_path)
    final_path = data_path / f"{nombre}.parquet"
    mascara = gpd.GeoDataFrame(mascara)
    mascara.to_parquet(final_path, index = False)

        
