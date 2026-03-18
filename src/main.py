import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import asyncio
from dotenv import load_dotenv
import traceback
from extraccion import mascaras, minioFunctions
from shapely.geometry import box
import geopandas as gpd

# Función encargada de unificar y facilitar el debug de cada módulo, avisar de imports faltantes y diferentes rutas


#Sacamos el path actual, su padre y esa será la ruta donde se buscan los otros paquetes
src_path = Path(__file__).parent
sys.path.append(str(src_path))

#Cargas desde el INICIO todas las claves de entorno, por si se llaman
load_dotenv()

# CONFIGURACIÓN DE EARTH ENGINE
def setup_earth_engine():
    """Configura Earth Engine usando la variable RUTA_CREDENCIALES."""

    try:
        import ee

        ruta_creds = os.getenv('RUTA_CREDENCIALES')
        
        print(f"\n🔑 Configurando Earth Engine...")
        print(f"   📁 RUTA_CREDENCIALES = {ruta_creds}")

        if not ruta_creds:
            print("   CUIDADO!!! Variable RUTA_CREDENCIALES no definida en .env")
            return False

        if not os.path.exists(ruta_creds):
            print(f"   CUIDADO!!! La ruta no existe: {ruta_creds}")
            return False

        if os.path.isfile(ruta_creds) and ruta_creds.endswith('.json'):
            json_file = ruta_creds
        elif os.path.isdir(ruta_creds):
            json_files = list(Path(ruta_creds).glob('*.json'))
            if not json_files:
                print("   CUIDADO!!! No se encontraron archivos .json en el directorio")
                return False
            json_file = str(json_files[0])
            print(f"   📄 Usando credenciales: {json_files[0].name}")
        else:
            print("   CUIDADO!!! La ruta no es un archivo JSON ni un directorio")
            return False

        try:
            credentials = ee.ServiceAccountCredentials(None, json_file)
            ee.Initialize(credentials)
            print("   ✅ Earth Engine inicializado correctamente")
            return True
        
        except Exception as e:
            print(f"   CUIDADO!!! Error al inicializar Earth Engine: {e}")
            return False

    except ImportError:
        print("      CUIDADO!!! Earth Engine no instalado. Ejecuta: pip install earthengine-api")
        return False

EE_OK = setup_earth_engine()

MODULOS_CARGADOS = False
try:
    print("\n Importando módulos de extraccion.Comprobación de uv sync")

    print("   incendios")
    from extraccion import incendios
    print("   ✅ OK")

    print("   construccion_df")
    from extraccion import construccion_df
    print("   ✅ OK")

    print("   vegetacion")
    from extraccion import vegetacion
    print("   ✅ OK")

    print("   pendiente")
    from extraccion import pendiente
    print("   ✅ OK")

    print("   fisicas")
    from extraccion import fisicas
    print("   ✅ OK")

    print("   vegetacion2")

    from extraccion.descartadas import vegetacion2
    print("   ✅ OK")

    print("   puntos_sinteticos")
    from extraccion import puntos_sinteticos
    print("   ✅ OK")

    print("   suelo")

    from extraccion.descartadas import suelo
    print("   ✅ OK")

    print("   suelo2")
    from extraccion.futuro import suelo2
    print("   ✅ OK")

    print("   civilizacion")
    from extraccion.futuro import civilizacion
    print("   ✅ OK")


    MODULOS_CARGADOS = True
    print("\n BIEN: Todos los módulos cargados correctamente.\n")

except Exception as e:
    print(f"\n ERROR: Error al importar módulos: {e}")
    traceback.print_exc()
    MODULOS_CARGADOS = False
    input("\nPresiona Enter para continuar...")

def limpiar_pantalla():
    os.system('cls' if os.name == 'nt' else 'clear')

def formatear_ruta(ruta, max_len=50):
    if not ruta:
        return "No definida"
    if len(ruta) > max_len:
        return ruta[:max_len] + "..."
    return ruta

# Función para obtener parámetros
def obtener_parametros():
    """Pregunta al usuario si quiere especificar parámetros y los devuelve."""
    print("\n--- Personalización de parámetros ---")
    resp = input("¿Desea especificar parámetros personalizados? (s/n): ").strip().lower()
    if resp != 's':
        return None, None, None   

    limit_input = input("limit (número entero, dejar vacío para 20, -1 para todas las filas): ").strip()
    try:
        limit = int(limit_input) if limit_input else 20
    except ValueError:
        print("Valor no válido, se usará 20 por defecto.")
        limit = 20

    # Solicitar fecha_ini
    fecha_ini = input("fecha_ini (formato YYYY-MM-DD, vacío para None): ").strip()
    fecha_ini = fecha_ini if fecha_ini else None

    # Solicitar fecha_fin
    fecha_fin = input("fecha_fin (formato YYYY-MM-DD, vacío para None): ").strip()
    fecha_fin = fecha_fin if fecha_fin else None

    return limit, fecha_ini, fecha_fin

async def mostrar_menu():
    limpiar_pantalla()
    print("\n" + "-"*60)
    print("  SISTEMA DE ANÁLISIS DE INCENDIOS ")
    print("-"*60)

    ruta_creds = os.getenv('RUTA_CREDENCIALES', 'No definida')

    print(f"\n📁 RUTA_CREDENCIALES: {formatear_ruta(ruta_creds)}")
    print(f" Módulos: {'✅ Cargados' if MODULOS_CARGADOS else 'ERROR: No disponibles'}")
    print(" "*60)

    print("\n📋 MENÚ PRINCIPAL:")
    if MODULOS_CARGADOS:
        print("  1. Construcción DF Ambiental (parámetros: limit, fechas)")
        print("  2. Vegetación (parámetros: limit, fechas)")
        print("  3. Pendiente (parámetros: limit, fechas)")
        print("  4. Características Físicas (parámetros: limit, fechas)")
    else:
        print("  ->  Módulos no disponibles (ejecuta opción 6 para diagnosticar)")
    print("  5. Información del Proyecto")
    print("  6. Diagnosticar Sistema")
    print("  7. Cambiar ruta para la extracción de datos")
    if MODULOS_CARGADOS:
        print("  8. Incendios")
        print("  9. Generar puntos sintéticos (requiere archivo Parquet)")
        print("  10. Concatenar un bucket de alguna característica (requiere archivos Parquet)")
        print("  11. Juntar todas las variables por año (merge)")
        print("  12. Nueva variable suelo2")
        print("  13. Nueva variable civilizacion (por terminar)")
    print("  0. Salir")
    print(" "*60)

async def diagnosticar_sistema():

    print("\n🔍 DIAGNÓSTICO COMPLETO")
    print(" "*50)

    print(f"Python: {sys.version}")
    print(f"Directorio actual: {os.getcwd()}")
    print(f"src path: {src_path}")

    print("\n Variables de entorno (.env):")

    ruta_creds = os.getenv('RUTA_CREDENCIALES')

    print(f"   RUTA_CREDENCIALES: {'BIEN' if ruta_creds else 'MAL'} {ruta_creds}")

    if ruta_creds:
        print(f"\n📁 Verificando RUTA_CREDENCIALES:")
        if os.path.exists(ruta_creds):
            print(f"      Existe")
            if os.path.isfile(ruta_creds):
                print(f"   📄 Es archivo")
            else:
                print(f"   📁 Es directorio")
                json_files = list(Path(ruta_creds).glob('*.json'))
                if json_files:
                    print(f"      Archivos JSON encontrados: {[f.name for f in json_files]}")
                else:
                    print(f"      No hay archivos .json")
        else:
            print(f"   ERROR: No existe")


    print(f"\n📦 Módulos de Python:")
    modulos = [
        ('ee', 'earthengine-api'),
        ('pandas', 'pandas'),
        ('numpy', 'numpy'),
        ('dotenv', 'python-dotenv'),
        ('aiohttp', 'aiohttp')
    ]
    for mod, pip in modulos:
        try:
            __import__(mod)
            print(f"   BIEN: {mod}")
        except ImportError:
            print(f"   ERROR {mod} (pip install {pip})")

    print(f"\n    Earth Engine inicializado: {'✅ Sí' if EE_OK else '❌ No'}")

async def ejecutar_funcion(nombre, func, *args, **kwargs):
    print(f"Ejecutando: {nombre}")
    try:
        resultado = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
        print(f"{nombre} completada.")
        return resultado
    except Exception as e:
        print(f"Error en {nombre}: {e}")

def pedirDatos():

    cliente = minioFunctions.crear_cliente()
    
    tipo_ruta = input("""
    Elige la ruta que quieras usar:
          0- Incendios + NoIncendios
          1- NoIncendios
          2- Incendios
          Otro input - Path diferente

    Selección: """)
    if tipo_ruta == "0":
        path_server = "grupo3/raw/Incendios_y_no_incendios/"
        nombre = input(f"Introduce el nombre del archivo para completar la ruta {path_server}")
        path_server = f"{path_server}{nombre}"
    elif tipo_ruta == "1":
        path_server = "grupo3/raw/No_incendios/"
        nombre = input(f"Introduce el nombre del archivo para completar la ruta {path_server}")
        path_server = f"{path_server}{nombre}"
    elif tipo_ruta == "2":
        path_server = "grupo3/raw/incendios/"
        nombre = input(f"Introduce el nombre del archivo para completar la ruta {path_server}")
        path_server = f"{path_server}{nombre}"
    else:
        path_server = input("Introduce la ruta al parquet que quieres usar (grupo3/raw/.../.parquet): ")
    
    tipo_retorno = input("""Introduce el tipo de documento que quieres que devuelva (df, gdf, parquet, csv). (Recomendado DF)
                         Si quieres comenzar con un nuevo csv seleccione esta opción y construccion_df
                         Nuestras funciones utilizan DF:            
                         """).strip().lower()
    
    devolver_parquet = False
    if tipo_retorno == "parquet":
        devolver_parquet = True
        tipo_descarga = "df"  
    elif tipo_retorno == "csv":
        df = minioFunctions.bajar_csv(cliente, path_server,sep=',', encoding='utf-8', header=0)
        return df
    else:
        tipo_descarga = tipo_retorno  
    
    try:
        df = minioFunctions.bajar_fichero(cliente, path_server, tipo_descarga)
    except Exception as e:
        print(f"Error al descargar el fichero: {e}")
        return None
    
    if devolver_parquet:
        parquet_bytes = df.to_parquet() 
        return parquet_bytes
    else:
        print(df["date_first"].head())
        return df

# MAIN
async def main():
    df_incendios = None
    pregunta = True

    while True:
        await mostrar_menu()
        opcion = input("\n🔷 Selecciona una opción (0-13): ").strip()

        if pregunta and opcion not in ["0", "5", "6", "8",'10', '11', '13']:
            resultado = pedirDatos()
            pregunta = False

            if resultado is not None:
                df_incendios = resultado
                print(f"Recuerda que esta ruta se utilizará en todas las operaciones posteriores")
            else:
                print(f"No se consiguió tener el documento")
        
        
        # Permite modificar parámetros
        if opcion == "1" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()

            if limit is None:
                await ejecutar_funcion("Construcción DF Ambiental", construccion_df.build_environmental_df, df_incendios)
            else:
                await ejecutar_funcion("Construcción DF Ambiental", construccion_df.build_environmental_df, 
                                       df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin, directo = True)

        elif opcion == "2" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Vegetación", vegetacion.df_vegetacion, df_incendios)
            else:
                await ejecutar_funcion("Vegetación", vegetacion.df_vegetacion, 
                                       df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "3" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Pendiente", pendiente.df_pendiente, df_incendios)
            else:
                await ejecutar_funcion("Pendiente", pendiente.df_pendiente, 
                                       df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "4" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Características Físicas", fisicas.df_fisicas, df_incendios)
            else:
                await ejecutar_funcion("Características Físicas", fisicas.df_fisicas, 
                                       df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "5":
            print("\n" + " "*60)
            print(" INFORMACIÓN DEL PROYECTO")
            print(" "*60)
            print("""
            Este código es utilizado para la extracción completa de datos.

            A tener en cuenta que cada extracción puede ser subida a MinIO si así lo desea su creador.
            Todo está automatizado, siendo el uso de rutas en .env utilizadas para pruebas sin conexión con el servidor.

            Esta se reparte de la siguiente manera:

            - Main: Compuesto por un menú que indica dependencias, librerías y diferentes funciones.
            - construccion_df: Se le pasa una ruta de MinIO y construye un DataFrame y un parquet completo con todas las variables a estudiar.
            - fisicas.py: Saca las características físicas al mandar una ruta a un .parquet con la API Open-Meteo.
            - incendios.py: Extrae, limpia y aúna los datos de cada incendio al obtener una ruta de MinIO.
            - pendiente.py: Extrae los datos de la pendiente al mandar una ruta .parquet con los satélites de Google Earth Engine.
            - vegetacion.py: Extrae los datos de la vegetación al mandar una ruta .parquet con la API de Google Earth.
            - vegetacion2.py: Analiza los datos mediante una rasterización de un .tif para saber si se encuentra en agua, zona urbana o en qué tipo de vegetación se encuentra.
            - puntos_sinteticos.py: Creación de puntos por incendio basado en cercanía, área, intensidad de incendios y aleatoriedad.
            - filtros_no_sinteticos.py: Funciones para filtrar la creación de puntos sintéticos.
            - mascaras.py: Diferentes funciones de parse y de filtro de máscaras y parquets.
            - minioFunctions.py: Funciones para subir, bajar y manejar archivos en MinIO sin tener que tenerlos en local.
            - parquet.py: Función para ordenar parquets dentro de MinIO.

            """)

            print("="*60)

        elif opcion == "6":
            await diagnosticar_sistema()

        elif opcion == "7":
            resultado = pedirDatos()
            if resultado is not None:
                df_incendios = resultado
                print(f"Ruta guardada")
            else:
                print(f"Fallo al guardar la ruta")
            continue

        elif opcion == "8" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
        
            await ejecutar_funcion("Incendios", incendios.fetch_fires,
                                    df_incendios, fecha_ini=fecha_ini, fecha_fin=fecha_fin, question=True)
            
        elif opcion == "9" and MODULOS_CARGADOS:

            print(f"\n📊 Generando puntos sintéticos")

            try:
                
                # Es un hilo separado para no molestar la sincronización

                df_resultado = await asyncio.to_thread(puntos_sinteticos.crearSinteticos, df_incendios)
                print(f"\n   Se generaron {len(df_resultado)} puntos sintéticos.")
                print("\nPrimeras 10 filas:")
                print(df_resultado.head(10))
                
                guardar = input("\n¿Guardar resultado en CSV? (s/n): ").strip().lower()
                if guardar == 's':
                    nombre_csv = input("Nombre del archivo CSV (vacío para 'sinteticos.csv'): ").strip()
                    if not nombre_csv:
                        nombre_csv = "sinteticos.csv"
                    df_resultado.to_csv(nombre_csv, index=False)
                    print(f"   Guardado en {nombre_csv}")
            except Exception as e:
                print(f"   Error durante la generación: {e}")
                traceback.print_exc()    

        elif opcion == "10":
            df = await construccion_df.concatenar_df()
            print(df)

        elif opcion == "11" and MODULOS_CARGADOS:
            anios = [2022, 2023, 2024, 2025]
            for anio in anios:
                construccion_df.merge_parquets([
                f"grupo3/raw/Incendios_y_no_incendios/incendios_y_no_incendios_{anio}.parquet",
                f"grupo3/raw/Pendiente/incendios_y_no_incendios_Pendiente_{anio}.parquet",
                f"grupo3/raw/Fisicas/fisicas_{anio}_concat.parquet",
                f"grupo3/raw/Vegetacion/incendios_y_no_incendios_Vegetacion_{anio}.parquet"
                ], anio = anio)
        elif opcion == "12":
            limit, fecha_ini, fecha_fin = obtener_parametros()
        
            await ejecutar_funcion("Suelo", suelo2.df_soil_temp, df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "13" and MODULOS_CARGADOS:
            await ejecutar_funcion("Civilización", civilizacion.civilizacion, df_incendios)


        elif opcion == "0":
            print("\n   ¡Adios! Pasa un buen día ")
            break
        else:
            print("\n ERROR: Opción no válida o módulos no cargados.")

        input("\n⏎ Presiona Enter para continuar...")

if __name__ == "__main__":
    
    asyncio.run(main())