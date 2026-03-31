import pandas as pd
from extraccion import minioFunctions as mf
import h3

def tomar_dataframe():
    '''
    Función para tomar el dataframe desde MinIO.
    :return df: dataframe.
    '''
    cliente = mf.crear_cliente()
    df = mf.bajar_fichero(cliente, "grupo3/cleaned/MINI.parquet", "df")
    return df

def transformacion_h3(df, resolucion = 7):
    '''
    Transformar latitud y longitud en hexágonos.

    :param df: dataframe.
    :return df: dataframe con columna 'id_hexagono'.
    '''

    #Creación de los hexágonos
    df['id_hexagono'] = [
        h3.latlng_to_cell(lat, lon, resolucion) 
        for lat, lon in zip(df['lat'], df['lon'])
    ]   

    print("Datos con el nuevo identificador espacial:")
    print(df[['lat', 'lon', 'id_hexagono']].head())

    df_agrupado = df.groupby(['id_hexagono', 'date']).agg(
        #Variable respuesta: si en el hexágono hay al menos un incendio, se marca como 1
        incendio=('final', 'max'),
        
        # Coordenadas: Media 
        lat=('lat', 'mean'),
        lon=('lon', 'mean'),

        # Fecha: First (porque la fecha es la mismoa para cada hexágono en cada día)
        dia_sin=('dia_sin', 'first'),
        dia_cos=('dia_cos', 'first'),

        #Topografía: Media
        elevacion_centro=('elevacion_centro', 'mean'),
        grados=('grados', 'mean'), 
        porcentaje=('porcentaje', 'mean'),
        soil_temp=('soil_temp', 'mean'),
        dist_civ=('dist_civ', 'mean'),

        #Metereología: Máximos y mínimos
        wind_speed_max=('wind_speed_max', 'max'), 
        wind_gusts_max=('wind_gusts_max', 'max'),
        temp_max=('temp_max', 'max'),
        temp_min=('temp_min', 'min'), 
        
        #Metereología: Media
        pressure_mean=('pressure_mean', 'mean'),
        cloud_cover=('cloud_cover', 'mean'),
        radiation=('radiation', 'mean'),
        evapotranspiration=('evapotranspiration', 'mean'),
        sunshine_seconds=('sunshine_seconds', 'mean'),
        humidity_mean=('humidity_mean', 'mean'),
        precipitation=('precipitation', 'mean'),
        temp_mean=('temp_mean', 'mean'),

        #Vegetación: Media
        NDVI=('NDVI', 'mean'),
        NDWI=('NDWI', 'mean')    
        
    ).reset_index()
   
    print("Dataframe agrupado:")
    print(df_agrupado.head())

    return df_agrupado


if __name__ == "__main__":
    # Tomamos el dataframe original
    df = tomar_dataframe()
    
    # Transformamos el dataframe con hexágonos
    df_hex = transformacion_h3(df)
    print("Dataframe final con hexágonos:")
    print(df_hex.head())

    # Lo subimos a MinIO
    cliente = mf.crear_cliente()
    mf.subir_fichero(cliente, "grupo3/cleaned/MINI_h3.parquet", df_hex)

    