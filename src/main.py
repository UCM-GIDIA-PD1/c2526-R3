import sys
import os
from pathlib import Path
from modelos.regresion.arboles import frp_rdForest
import pandas as pd
import numpy as np
import asyncio
from dotenv import load_dotenv
import traceback
from extraccion import mascaras, minioFunctions
from shapely.geometry import box
import geopandas as gpd
from limpieza import limpieza
from modelos.evaluacion import evaluacion_final
from modelos.clasificacion import decisiontree, balanced_random_forest, random_forest, m_xgboost,  regresion_logistica
from modelos.generico import modelo_xgboost
from modelos.regresion.arboles import frp_xgBoost

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
        
        print(f"\n Configurando Earth Engine...")
        print(f"RUTA_CREDENCIALES = {ruta_creds}")

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

    print("   puntos_no_incendio")
    from extraccion import puntos_no_incendio
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

    print("   ganado")
    from extraccion.futuro import ganado
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
        print("  5. Extraer de variables descartadas")
        print("  6. Extraer de variables futuro")
    else:
        print("  ->  Módulos de extracción no disponibles (ejecuta opción 16 para diagnosticar)")
        
    if MODULOS_CARGADOS:
        print("  7. Incendios")
        print("  8. Generar puntos sintéticos (requiere archivo Parquet)")
        print("  9. Concatenar buckets de características (requiere archivos Parquet)")
        print("  10. Juntar todas las variables por año (merge)")
        print("  11. Extraer máscaras faltantes")
        print("  12. Limpieza de valores nulos")
        print("  13. Evaluar modelo final")
        print("  14. Entrenar modelo")
        
    print("  15. Información del Proyecto")
    print("  16. Diagnosticar Sistema")
    print("  17. Cambiar ruta para la extracción de datos")
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
        print(df["date"].head())
        return df

# MAIN
async def main():
    df_incendios = None
    pregunta = True

    # Opciones que no requieren pedir datos por defecto al inicio
    exclusiones_pedir_datos = ["0", "7", "9", "10", "11", "13", "14", "15", "16", "17"]

    while True:
        await mostrar_menu()
        opcion = input("\n🔷 Selecciona una opción (0-17): ").strip()

        if pregunta and opcion not in exclusiones_pedir_datos:
            resultado = pedirDatos()
            pregunta = False

            if resultado is not None:
                df_incendios = resultado
                print(f"Recuerda que esta ruta se utilizará en todas las operaciones posteriores")
            else:
                print(f"No se consiguió tener el documento")
        
        # Opciones de Extracción Directa y Construcción
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

        elif opcion == "5" and MODULOS_CARGADOS:
            print("\n--- Variables Descartadas ---")
            print(" 1. Vegetación 2")
            print(" 2. Suelo")
            sub_op = input("Elige la variable que deseas extraer: ").strip()
            limit, fecha_ini, fecha_fin = obtener_parametros()

            if sub_op == "1":
                await ejecutar_funcion("Vegetación 2 (Descartada)", vegetacion2.df_vegetacion2, df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)
            elif sub_op == "2":
                await ejecutar_funcion("Suelo (Descartada)", suelo.df_suelo, df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)
            else:
                print("Opción no válida.")

        elif opcion == "6" and MODULOS_CARGADOS:
            print("\n--- Variables Futuro ---")
            print(" 1. Suelo 2")
            print(" 2. Civilización")
            print(" 3. Ganado")
            sub_op = input("Elige la variable que deseas extraer: ").strip()
            
            if sub_op == "1":
                limit, fecha_ini, fecha_fin = obtener_parametros()
                await ejecutar_funcion("Suelo 2 (Futuro)", suelo2.df_soil_temp, df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)
            elif sub_op == "2":
                await ejecutar_funcion("Civilización (Futuro)", civilizacion.civilizacion, df_incendios)
            elif sub_op == "3":
                limit, fecha_ini, fecha_fin = obtener_parametros()
                await ejecutar_funcion("Ganado (Futuro)", ganado.df_ganado, df_incendios, limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)
            else:
                print("Opción no válida.")

        elif opcion == "7" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            await ejecutar_funcion("Incendios", incendios.fetch_fires,
                                    df_incendios, fecha_ini=fecha_ini, fecha_fin=fecha_fin, question=True)
            
        elif opcion == "8" and MODULOS_CARGADOS:
            print(f"\n📊 Generando puntos sintéticos")
            try:
                # Es un hilo separado para no molestar la sincronización
                df_resultado = await asyncio.to_thread(puntos_no_incendio.crearSinteticos, df_incendios)
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

        elif opcion == "9":
            df = await construccion_df.concatenar_df()
            print(df)

        elif opcion == "10" and MODULOS_CARGADOS:
            anios = [2022, 2023, 2024, 2025]
            for anio in anios:
                construccion_df.merge_parquets([
                f"grupo3/raw/Incendios_y_no_incendios/incendios_y_no_incendios_{anio}.parquet",
                f"grupo3/raw/Pendiente/incendios_y_no_incendios_Pendiente_{anio}.parquet",
                f"grupo3/raw/Fisicas/fisicas_{anio}_concat.parquet",
                f"grupo3/raw/Vegetacion/incendios_y_no_incendios_Vegetacion_{anio}.parquet"
                ], anio = anio)

        elif opcion == "11":
            mascaras.extraer_mascaras_faltantes()

        elif opcion == "12" and MODULOS_CARGADOS:
            nulos = input("¿Quieres ver los valores nulos? (s/n): ").strip()

            if nulos == "s":
                print("\n Analizando valores nulos...")
                
                resumen_nulos = await asyncio.to_thread(limpieza.mostrar_nulos, df_incendios)
                
                if resumen_nulos is None:
                    print("El DataFrame no tiene valores nulos.")
                else:
                    print("\n Columnas con valores nulos detectadas:")
                    print("-" * 40)
                    for col, cantidad in resumen_nulos.items():
                        porcentaje = (cantidad / len(df_incendios)) * 100
                        print(f" {col.ljust(20)} | {str(cantidad).rjust(7)} nulos ({porcentaje:.2f}%)")
                    print("-" * 40)
                
            confirmar_limpieza = input("\n¿Quieres realizar ya la limpieza? (s/n): ").strip()
            if confirmar_limpieza == "s":
                print("\n Opciones de limpieza por categorías:")
                print("  1. Físicas (Temperatura, Viento, Presión, etc.)")
                print("  2. Pendiente (Grados, Elevación, etc.)")
                print("  3. Suelo (Temperatura del suelo)")
                print("  4. Vegetación (NDVI, NDWI)")
                print("  5. Distancia a la civilización")
                print("  6. Otras (especificar nombres de columnas)")

                entrada = input("\nSelecciona números (separados por coma y sin espacios) o Enter para TODAS: ").strip()

                mapeo_categorias = {
                    "1": ['temp_mean', 'temp_max', 'temp_min', 'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
                            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
                            'sunshine_seconds'],
                    "2": ['porcentaje', 'grados', 'elevacion_centro'],
                    "3": ['soil_temp'],
                    "4": ['NDVI', 'NDWI'],
                    "5": ['dist_civ']
                }

                cols_finales = []
                if entrada:
                    selecciones = [s.strip() for s in entrada.split(",")]
                    for s in selecciones:
                        if s in mapeo_categorias:
                            cols_finales.extend(mapeo_categorias[s])
                        elif s == "6":
                            entrada2 = input("\nEscribe las columnas que quieres eliminar para la opción 6 (separados por coma y sin espacios): ").strip()
                            if entrada2:
                                columnas = [s.strip() for s in entrada2.split(",")]
                                cols_finales.extend(columnas) 
                    cols_finales = list(set([c for c in cols_finales if c in df_incendios.columns]))
                else:
                    cols_finales = None

                print(" Procesando limpieza...")
                filas_antes = len(df_incendios)
                
                df_incendios = await asyncio.to_thread(limpieza.limpieza_nulos, df_incendios, cols_finales)
                
                filas_despues = len(df_incendios)
                print(f"\n ¡Limpieza completada!")
                print(f" Filas eliminadas: {filas_antes - filas_despues}")
                print(f" Filas restantes: {filas_despues}")
    
        elif opcion == "13":
            print("\n--- Evaluación de Modelos Finales ---")
            print('Selecciona el tipo de modelo que quieres evaluar:')
            print("1. Modelos de clasificación (predicción de incendios)")
            print("2. Modelos de regresión (predicción del FRP)")
            tipo_modelo = input("\nIndica el tipo de modelo (1 para clasificación, 2 para regresión): ").strip()
            if tipo_modelo == "1":
                print("1.XGBoostClassifier.")
                print("2.BalancedRandomForest.")
                print("3.DecisionTree.")
                print("4.RandomForestClassifier")
                print("5.Regresión logística.")

                modelos = ["XGBoostClassifier", "BalancedRandomForest", "DecisionTree", "RandomForest", "Regresión logística"]
                modelo = input("\n Indica el modelo que quieres evaluar (el número): ")
                evaluacion_final.evaluacion_modelo(modelos[int(modelo) - 1])
            else:
                print("1.RandomForestFRP.")
                print("2.XGBoostFRP.")
                modelos = ["RandomForestFRP", "XGBoostFRP"]
                modelo = input("\n Indica el modelo que quieres evaluar (el número): ")
                evaluacion_final.evaluacion_modelo(modelos[int(modelo) - 1])
            

        elif opcion == "14":
            print("\n--- Entrenamiento de modelos ---")
            print('Selecciona el tipo de modelo que quieres entrenar:')
            print("1. Modelos de clasificación (predicción de incendios)")
            print("2. Modelos de regresión (predicción del FRP)")
            tipo_modelo = input("\nIndica el tipo de modelo (1 para clasificación, 2 para regresión): ").strip()

            if tipo_modelo == '1':
                print("1.XGBoostClassifier.")
                print("2.BalancedRandomForest.")
                print("3.DecisionTree.")
                print("4.RandomForestClassifier")
                print("5.Regresión logística.")
                modelo = input("\n Indica el modelo que quieres entrenar (el número): ")
                metodo = input("Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros:" )
                metrica = input("Selecciona la métrica que quieres optimizar (f1/f2):" )
                if modelo == '1':
                    decisiontree.clasificacion(metodo, metrica)
                elif modelo == '2':
                    ventanas_temporales = input("\nIndica si quieres ventanas temporales (s/n): ")
                    if ventanas_temporales == "s":
                        modelo_xgboost.entrenar() 
                    else:
                        m_xgboost.clasificacion(metodo, metrica)
                elif modelo == '3':
                    balanced_random_forest.clasificacion(metodo, metrica)
                elif modelo == '4':
                    random_forest.clasificacion(metodo, metrica)
                else:
                    regresion_logistica.clasificacion(metodo, metrica)
            else:
                print("1.RandomForestFRP.")
                print("2.XGBoostFRP.")
                modelo = input("\n Indica el modelo que quieres entrenar (el número): ")
                metodo = input("Selecciona el metodo (grid, random o bayes) para la búsqueda de hiperparámetros:" )
                metrica = input("Selecciona la métrica que quieres optimizar (RMSE/MAE/R2):" )
                if modelo == '1':
                    frp_rdForest.regresion(metodo, metrica) 
                else:
                    frp_xgBoost.regresion(metodo, metrica) 

        elif opcion == "15":
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
            - puntos_no_incendio.py: Creación de puntos por incendio basado en cercanía, área, intensidad de incendios y aleatoriedad.
            - filtros_no_incendio.py: Funciones para filtrar la creación de puntos de no incendio.
            - mascaras.py: Diferentes funciones de parse y de filtro de máscaras y parquets.
            - minioFunctions.py: Funciones para subir, bajar y manejar archivos en MinIO sin tener que tenerlos en local.
            - parquet.py: Función para ordenar parquets dentro de MinIO.

            """)
            print("="*60)

        elif opcion == "16":
            await diagnosticar_sistema()

        elif opcion == "17":
            resultado = pedirDatos()
            if resultado is not None:
                df_incendios = resultado
                print(f"Ruta guardada")
                pregunta = False
            else:
                print(f"Fallo al guardar la ruta")
            continue

        elif opcion == "0":
            print("\n   ¡Adios! Pasa un buen día ")
            break
        else:
            print("\n ERROR: Opción no válida o módulos no cargados.")

        input("\n⏎ Presiona Enter para continuar...")

if __name__ == "__main__":
    asyncio.run(main())