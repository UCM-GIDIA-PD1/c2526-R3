from extraccion import minioFunctions
import pandas as pd

def guardar_parcial(df, prefijo="parcial"):
    """
    Pregunta al usuario si desea guardar el DataFrame en local o subirlo a MinIO.
    Si se elige MinIO, se usa la ruta 'grupo3/interrupciones/'.
    
    :param df: DataFrame con los resultados parciales a guardar
    :param prefijo: Nombre del archivo si se guarda en local
    :return None:
    """
    print("\n Resultados parciales obtenidos")
    print(f"Filas: {len(df)}")
    opcion = input("¿Guardar en local (L) o subir a MinIO (M)? [L/M]: ").strip().upper()
    if opcion == 'M':
        minioFunctions.preguntar_subida(df, "grupo3/interrupciones/")
    else:
        nombre_archivo = f"{prefijo}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(nombre_archivo, index=False)
        print(f"Archivo guardado localmente como: {nombre_archivo}")