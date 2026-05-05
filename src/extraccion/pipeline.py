from extraccion import minioFunctions, incendios, filtros_no_incendio, puntos_no_incendio, construccion_df
from limpieza import limpieza, transformacion
import pandas as pd
import asyncio

def extraccion_de_incendios(anio=None):
    '''
    Función para extraer los datos de incendios desde MinIO, procesarlos y 
    devolver un DataFrame con los incendios agrupados.

    :param anio: Año de los incendios a procesar
    :return df_procesado: DataFrame con los incendios procesados y agrupados
    '''

    assert isinstance(anio, int), "El año debe ser un número entero."
    print("====================> PASO 1 : EXTRACCIÓN DE INCENDIOS ...")

    # Extracción de datos desde MinIO
    print("Descargando datos crudos desde MinIO...")
    cliente = minioFunctions.crear_cliente()
    
    # Procesamos año individual
    df_raw = minioFunctions.bajar_csv(cliente, f"grupo3/raw/incendios/{anio}.csv", sep=',')
    assert df_raw is not None, "No se pudo descargar el DataFrame de MinIO, verifica la conexión o si el año es correcto."
    print(f"\n>>>>>>>>>>Número de registros inicial: {len(df_raw)} <<<<<<<<<<<\n")
        
    # Procesamiento de los incendios 
    df_procesado = incendios.fetch_fires(df_raw, question=False, anio=anio)
    print(f"\n>>>>>>>>>>Número de registros tras agrupar los incendios: {len(df_procesado)} <<<<<<<<<<<\n")

    assert df_procesado is not None, "La función fetch_fires devolvió un DataFrame vacío, se esperaba un DataFrame con datos."
    print("Cabecera del DataFrame procesado:")
    print(df_procesado.head())

    return df_procesado

def filtracion_por_zonas(df_procesado):
    '''
    Función para filtrar los incendios por zonas utilizando las máscaras de MinIO.

    :param df_procesado: DataFrame con los incendios procesados
    :return df_inc: DataFrame con los incendios filtrados por zonas
    '''

    assert isinstance(df_procesado, pd.DataFrame), "El argumento de la función debe ser un DataFrame."
    print("\n====================> PASO 2 : FILTRACIÓN DE LOS INCENDIOS ...")
    
    # Listamos las máscaras por las que queremos filtrar los incendios
    cliente = minioFunctions.crear_cliente()
    mascaras = minioFunctions.listar_bucket(cliente, "grupo3/raw/Biogeoregiones/")
    mascaras += [
        'grupo3/raw/Countries/mascara_zona_Moscu.parquet',
        'grupo3/raw/Countries/mascara_San_Petersburgo.parquet',
        'grupo3/raw/Countries/mascara_Belarus.parquet',
        'grupo3/raw/Countries/mascara_Norte_Africa.parquet'
    ]

    # Filtramos los incendios por las zonas utilizando las máscaras
    df_procesado_zonas = filtros_no_incendio.filtrarZona(mascaras, df_procesado, cliente, devolver_lista=False)
    print(df_procesado_zonas.head())
    print(f"Longitud procesado: {len(df_procesado)}")
    print(f"Longitud procesado por zonas: {len(df_procesado_zonas)}")
    df_inc = df_procesado_zonas.copy()

    return df_inc

def generacion_no_incendios(df_inc, anio=None):
    '''
    Funcion para generar los puntos de no incendio, crear la variable respuesta y 
    concatenar los DataFrames de incendios y no incendios.

    :param df_inc: DataFrame con los incendios filtrados por zonas
    :return df_final: DataFrame final con los incendios y no incendios concatenados
    '''

    assert isinstance(df_inc, pd.DataFrame), "El argumento de la función debe ser un DataFrame."
    print("\n====================> PASO 3: GENERACIÓN DE NO INCENDIOS ...")
    
    # Generamos los puntos de no incendio
    df_no_inc = puntos_no_incendio.crearSinteticos(df_inc, subir = False)

    # Creamos la variable respuesta
    print(f"La proporción de incendios es: {round(len(df_no_inc) / (len(df_inc) + len(df_no_inc)) * 100, 2)}%")
    df_inc["final"] = 1
    print(f"La proporción de no incendios es: {round(len(df_inc) / (len(df_inc) + len(df_no_inc)) * 100, 2)}%")
    df_no_inc["final"] = 0

    # Concateanmos incendios y no incendios
    df_final = pd.concat([df_inc, df_no_inc], ignore_index=True)
    df_final['date'] = pd.to_datetime(df_final['date'])

    # Subimos a MinIO
    assert anio is not None, "Se requiere el año para subir a minio el archivo automáticamente"
    cliente = minioFunctions.crear_cliente()
    minioFunctions.subir_fichero(cliente, f"grupo3/raw/Incendios_y_no_incendios/incendios_y_no_incendios_{anio}.parquet", df_final)
    
    return df_final

async def extraccion(df_final, anio):
    '''
    Función para realizar la extracción de los datos
    físicos, de vegetación, pendiente, suelo y civilización, 
    concatenar las variables y devolver el DataFrame final.

    :param df_final: DataFrame con los incendios y no incendios concatenados
    :param anio: Año para subir el archivo a MinIO automáticamente
    :return df_entero: DataFrame final con todas las variables concatenadas
    '''

    assert isinstance(df_final, pd.DataFrame), "El argumento de la función debe ser un DataFrame."
    assert isinstance(anio, int), "El año debe ser un número entero."
    print("\n====================> PASO 4: EXTRACCIÓN, CONCATENACIÓN Y LIMPIEZA DE LAS VARIABLES ...")

    # Extraemos y concatenamos las variables
    resultados_extraccion = await construccion_df.extraccion_pipeline(df_final, anio=anio, limite_extraccion=-1)
    
    # Comprobamos que la extracción se ha realizado correctamente
    if resultados_extraccion is None:
        print("El pipeline de extracción se interrumpió y no devolvió los datos.")
        return 
        
    df_vegetacion, df_pendiente, df_fisicas, df_suelo2, df_civilizacion = resultados_extraccion
    df_entero = construccion_df.concatenar_variables(pipeline=True, anio=anio)

    df_entero = pd.merge(df_entero, df_final[['lat', 'lon', 'date', 'final', 'frp_mean']], on=['lat', 'lon', 'date'], how='left')
    df_entero = df_entero.drop(columns=['fire_index'], errors='ignore')

    cliente = minioFunctions.crear_cliente()
    def calcular_vpd(temp, rh):
        svp = 610.7 * (10**((7.5 * temp) / (237.3 + temp)))
        vpd = (1 - (rh / 100)) * svp
        return vpd

    # Subimos a MinIO los incendios
    df_incendios = df_entero[df_entero['final'] == 1]
    df_incendios['fuel_stress'] = df_incendios['NDVI'] - df_incendios['NDWI']
    df_incendios['VPD'] = calcular_vpd(df_incendios['temp_mean'], df_incendios['humidity_mean'])
    df_incendios['dry_fuel_index'] = df_incendios['NDVI'] / (df_incendios['NDWI'] + 1) 
    minioFunctions.subir_fichero(cliente, f"grupo3/raw/Final/final_incendios_{anio}.parquet", df_incendios)
    
    # Transformamos la variable fecha
    df_entero['dia_sin'], df_entero['dia_cos'] = transformacion.tranformar_date(df_entero, pipeline=True)
    df_entero = df_entero.drop(columns = ["frp_mean"], errors = 'ignore')
    # Subimos a MinIO
    minioFunctions.subir_fichero(cliente, f"grupo3/raw/Final/final_{anio}.parquet", df_entero)

    return df_entero

def limpieza_nulos(df_entero, anio = None):
    '''
    Función para limpiar los valores nulos del DataFrame final y subirlo a MinIO.
    
    :param df_entero: DataFrame final con todas las variables concatenadas
    :param anio: Año para subir el archivo a MinIO automáticamente
    :return df_limpio: DataFrame final limpio de valores nulos
    '''

    assert anio is not None, "Se requiere el año para ejecutar la limpieza de nulos en modo pipeline."
    print("\n====================> PASO 5: LIMPIEZA DE NULOS ...")
    print("\n Analizando valores nulos...")

    # Vemos número de nulos en el DataFrame
    resumen_nulos = limpieza.mostrar_nulos(df_entero)      

    if resumen_nulos is None:
        print("El DataFrame no tiene valores nulos.")
    else:
        print("\n Columnas con valores nulos detectadas:")
        print("-" * 40)
        for col, cantidad in resumen_nulos.items():
            porcentaje = (cantidad / len(df_entero)) * 100
            print(f" {col.ljust(20)} | {str(cantidad).rjust(7)} nulos ({porcentaje:.2f}%)")
        print("-" * 40)

    print(" Procesando limpieza...")
    filas_antes = len(df_entero)
    
    # Hacemos la limpieza
    df_limpio = limpieza.limpieza_nulos(df_entero, pipeline=True, anio=anio)

    filas_despues = len(df_limpio)
    print(f"\n ¡Limpieza completada!")
    print(f" Filas eliminadas: {filas_antes - filas_despues}")
    print(f" Filas restantes: {filas_despues}")

    # Subimos a MinIO
    cliente = minioFunctions.crear_cliente()
    minioFunctions.subir_fichero(cliente, f"grupo3/cleaned/final_{anio}.parquet", df_limpio)

    df_incendios = df_limpio[df_limpio['final'] == 1]
    minioFunctions.subir_fichero(cliente, f"grupo3/cleaned/final_incendios_{anio}.parquet", df_incendios)

    return df_limpio
                

async def pipeline(anio=None):
    '''
    Función para ejecutar el pipeline completo de construcción del dataframe.
    :param anio: Año datado de los incendios.
    '''
    assert anio is not None, "Se requiere el año para ejecutar el pipeline completo"

    # ====================> PASO 1 : EXTRACCIÓN DE INCENDIOS 
    df_procesado = extraccion_de_incendios(anio=anio)

    # ====================> PASO 2 : FILTRAMOS LOS INCENDIOS POR ZONAS
    df_inc = filtracion_por_zonas(df_procesado)
 
    # ====================> PASO 3 : GENERACIÓN DE NO INCENDIOS Y CONCATENACIÓN
    df_final = generacion_no_incendios(df_inc, anio=anio)

    # ====================> PASO 4 : EXTRACCIÓN, CONCATENACIÓN Y LIMPIEZA DE LAS VARIABLES
    df_entero = await extraccion(df_final, anio=anio)

    # ====================> PASO 5 : LIMPIEZA DE NULOS
    df_entero = limpieza_nulos(df_entero, anio=anio)



if __name__ == "__main__":
    pipeline(anio=2026)