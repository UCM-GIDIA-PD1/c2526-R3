import sys
import os
from pathlib import Path
from modelos.regresion.arboles import frp_rdForest
import asyncio
from dotenv import load_dotenv
import traceback
from extraccion import mascaras, minioFunctions, pipeline
from shapely.geometry import box
import geopandas as gpd
from limpieza import limpieza
from modelos.evaluacion import evaluacion_final
from modelos.clasificacion import decisiontree, balanced_random_forest, random_forest, m_xgboost, regresion_logistica, modelo_inversa, modelo_incremental
from modelos.generico import modelo_xgboost
from modelos.regresion.arboles import frp_xgBoost

src_path = Path(__file__).parent
sys.path.append(str(src_path))

load_dotenv()

def setup_earth_engine():
    """
    Configura Earth Engine usando la variable RUTA_CREDENCIALES.

    :return bool: True si se inicializó correctamente, False en caso contrario.
    """

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
    """
    Limpia la pantalla de la consola.
    
    :return: None
    """
    os.system('cls' if os.name == 'nt' else 'clear')

def formatear_ruta(ruta, max_len=50):
    """
    Recorta una ruta de archivo si excede la longitud máxima.

    :param ruta: La ruta de archivo a formatear.
    :param max_len: Longitud máxima permitida.
    :return str: La ruta formateada (posiblemente con puntos suspensivos).
    """
    if not ruta:
        return "No definida"
    if len(ruta) > max_len:
        return ruta[:max_len] + "..."
    return ruta

def obtener_parametros():
    """
    Pregunta al usuario si quiere especificar parámetros personalizados (limit, fechas).

    :return tuple: (limit, fecha_ini, fecha_fin) o (None, None, None) si no se personaliza.
    """
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

    fecha_ini = input("fecha_ini (formato YYYY-MM-DD, vacío para None): ").strip()
    fecha_ini = fecha_ini if fecha_ini else None

    fecha_fin = input("fecha_fin (formato YYYY-MM-DD, vacío para None): ").strip()
    fecha_fin = fecha_fin if fecha_fin else None

    return limit, fecha_ini, fecha_fin

async def mostrar_menu():
    """
    Muestra el menú principal en la consola con el estado del sistema.

    :return: None
    """
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

    print("\n📋 PIPELINE:")
    print("Para ejecutar el pipeline completo pulse 'P': ")

async def diagnosticar_sistema():
    """
    Realiza un diagnóstico completo del sistema (Python, variables de entorno, módulos, Earth Engine).

    :return: None
    """
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
    """
    Ejecuta una función (síncrona o asíncrona) de forma segura y muestra mensajes de estado.

    :param nombre: Nombre descriptivo de la operación.
    :param func: La función a ejecutar.
    :param args: Argumentos posicionales para la función.
    :param kwargs: Argumentos de palabra clave para la función.
    :return: El resultado de la ejecución de la función.
    """
    print(f"Ejecutando: {nombre}")
    try:
        resultado = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
        print(f"{nombre} completada.")
        return resultado
    except Exception as e:
        print(f"Error en {nombre}: {e}")

def pedirDatos():
    """
    Solicita al usuario la ruta del archivo de datos en MinIO y el formato de retorno deseado.

    :return: DataFrame, GeoDataFrame, bytes de Parquet o None según la selección.
    """
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

async def main():
    df_incendios = None
    pregunta = True

    exclusiones_pedir_datos = ["0", "7", "9", "10", "11", "13", "14", "15", "16", "17", "P", "p"]

    while True:
        await mostrar_menu()
        opcion = input("\n🔷 Selecciona una opción (0-17) o 'P' para iniciar el pipeline: ").strip()

        if pregunta and opcion not in exclusiones_pedir_datos:
            resultado = pedirDatos()
            pregunta = False

            if resultado is not None:
                df_incendios = resultado
                print(f"Recuerda que esta ruta se utilizará en todas las operaciones posteriores")
            else:
                print(f"No se consiguió tener el documento")
        
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
                print("6.Modelo Inversa (Filtro XGBoost).")
                print("7.Modelo Incremental.")
                modelo = input("\n Indica el modelo que quieres entrenar (el número): ")
                
                if modelo == '6':
                    print("\n--- Opciones Modelo Inversa ---")
                    print("1. Ejecutar Búsqueda Exhaustiva (Grid Sweep)")
                    print("2. Probar configuración Refinada (Existente)")
                    print("3. Probar CONFIGURACIÓN SEQUÍA (VPD)")
                    print("4. MODO EXPLORADOR AUTOMÁTICO (Combinaciones aleatorias/bayes)")
                    print("5. MODO EXPLORADOR ANTRÓPICO (Civilización + Clima)")
                    op_inversa = input("\nElige una opción (1-5): ")
                    
                    if op_inversa in ["1", "4", "5"]:
                        metrica_inv = input("Selecciona la métrica que quieres optimizar (f1/f2): ")
                        if op_inversa == "1":
                            metodo_inv = "grid"
                            print("Método fijado en: grid (Búsqueda Exhaustiva)")
                        else:
                            metodo_inv = input("Selecciona el metodo (grid, random o bayes) para la búsqueda: ")
                        iteraciones_inv = int(input("Introduce el número máximo de iteraciones: "))
                        modelo_inversa.clasificacion(op_inversa, metodo_inv, metrica_inv, iteraciones_inv)
                        
                    elif op_inversa in ["2", "3"]:
                        print("\nEjecutando configuración manual predefinida (1 sola iteración en WandB)...")
                        modelo_inversa.clasificacion(op_inversa, "grid", "f2", iteraciones=1)
                    else:
                        print("Opción no válida.")
                
                elif modelo == '7':
                    print("\nMODELO INCREMENTAL")
                    print("1. FASE 1: Alta Sensibilidad (Prop 0.1 - 0.3)")
                    print("2. FASE 2: Inyección de Ruido (Prop 0.4 - 0.8)")
                    print("3. FASE 3: Entorno Real (Prop 1.0 - 1.5)")
                    print("4. MODO ESCALADA COMPLETA (Prop 0.1 a 1.5)")
                    
                    opcion_inc = input("\nElige una fase a explorar (1-4): ")
                    
                    if opcion_inc in ["1", "2", "3", "4"]:
                        metrica_inc = input("Selecciona la métrica que quieres optimizar (f1/f2): ")
                        metodo_inc = input("Selecciona el metodo (grid, random o bayes): ")
                        iteraciones_inc = int(input("Introduce el número máximo de iteraciones: "))
                        
                        modelo_incremental.clasificacion_incremental(opcion_inc, metodo_inc, metrica_inc, iteraciones_inc)
                    else:
                        print("Opción no válida.")

                else:
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
             Este código es el núcleo centralizado para la extracción, limpieza, análisis y modelado de datos de incendios forestales.

            A tener en cuenta que cada extracción puede ser subida a MinIO si así lo desea su creador. Todo está automatizado, siendo el uso de rutas en .env utilizadas para pruebas sin conexión con el servidor.

            El proyecto cuenta con una arquitectura modular y se reparte de la siguiente manera:

             ARCHIVO PRINCIPAL (src/)
            - main.py: Menú interactivo CLI para la gestión masiva de datos y entrenamiento de modelos. Orquesta todo el pipeline desde la descarga de datos hasta la evaluación final.

             API REST Y SERVICIOS (app/)
            - main.py: Servidor backend basado en FastAPI. Implementa endpoints para predicción síncrona y mediante streaming (Server-Sent Events) para actualizaciones en tiempo real durante la extracción de variables.
            - schemas.py: Contratos de datos robustos usando Pydantic para validar latitud, longitud y fechas en las peticiones.
            - services/fire_service.py: El "cerebro" de la API. Gestiona la extracción dinámica de variables ambientales (NDVI, meteorología, topografía) para puntos geográficos arbitrarios y realiza la inferencia con los modelos XGBoost cargados desde MinIO.
            - mapa-ignis/: Dashboard interactivo de última generación. Desarrollado con Vite + React y Mapbox 3D, permite visualizar el riesgo de incendio en un mapa global y consultar el histórico de focos activos en tiempo real.

             EXTRACCIÓN DE DATOS (src/extraccion/)
            - construccion_df.py: Motor de ensamblaje de datasets. Combina múltiples fuentes en archivos Parquet optimizados para el entrenamiento.
            - fisicas.py / vegetacion.py / pendiente.py: Integración con APIs externas (Open-Meteo) y procesamiento de imágenes satelitales vía Google Earth Engine para obtener variables críticas como NDVI, NDWI, Temperatura, Humedad y Elevación.
            - incendios.py: Automatización de la descarga de datos históricos de incendios desde el sistema FIRMS de la NASA.
            - puntos_no_incendio.py: Lógica de submuestreo espacial para generar puntos de control (no incendio) y combatir el desbalanceo de clases intrínseco al problema.
            - minioFunctions.py: Gestión del ciclo de vida de los datos en el servidor de almacenamiento de objetos (MinIO), permitiendo la persistencia distribuida y el trabajo en equipo.

             LIMPIEZA Y PREPROCESAMIENTO (src/limpieza/)
            - limpieza.py: Pipeline de tratamiento de valores nulos, eliminación de columnas redundantes y aseguramiento de la consistencia de tipos.
            - transformacion.py: Ingeniería de características, normalización, escalado y codificación de variables temporales (ciclos estacionales).

             MODELADO PREDICTIVO (src/modelos/)
            - clasificacion/: Implementaciones de XGBoostClassifier, BalancedRandomForest y Regresión Logística para predecir la probabilidad de ignición. Incluye técnicas avanzadas de búsqueda de hiperparámetros (Grid, Random, Bayes).
            - regresion/: Algoritmos para estimar el Fire Radiative Power (FRP), permitiendo prever no solo si habrá fuego, sino su intensidad potencial.
            - evaluacion/: Generación automática de informes de rendimiento, matrices de confusión y curvas de importancia de características, integrándose con Weights & Biases (W&B) para el seguimiento de experimentos.

             ANÁLISIS Y EXPERIMENTACIÓN
            - analisis/: Laboratorio de ideas en formato Jupyter Notebook. Contiene el análisis exploratorio (EDA), pruebas de hipótesis y validación de nuevas fuentes de datos.
            - modelos/baseline/: Implementaciones de referencia para comparar el salto de rendimiento de los modelos finales.

             INFRAESTRUCTURA Y DESPLIEGUE
            - Dockerfile / Podman: Configuración para el empaquetado del sistema en contenedores ligeros y reproducibles.
            - .env: Gestión centralizada de secretos y rutas (WandB, MinIO, Google Earth Engine, Mapbox).
                  
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

        elif opcion == "P" or opcion == "p" and MODULOS_CARGADOS:
            print("¿Qué año deseas procesar?")
            anio = int(input("Año: "))

            # Comprobamos que estáb bien configurado MinIO para iniciar el pipeline.
            cliente = minioFunctions.crear_cliente()
            bucket = minioFunctions.listar_bucket(cliente, "grupo3/raw/incendios/")
            assert f"grupo3/raw/incendios/{anio}.csv" in bucket, f"No se encuentra el archivo {anio}.csv para iniciar el pipeline. Consulte el README para más información."
            print(f"Archivo de incendios {anio}.csv encontrado.")

            print("Ejecutando el pipeline... \n")
            await pipeline.pipeline(anio)
            


        elif opcion == "0":
            print("\n   ¡Adios! Pasa un buen día ")
            break
        else:
            print("\n ERROR: Opción no válida o módulos no cargados.")

        input("\n⏎ Presiona Enter para continuar...")

if __name__ == "__main__":
    asyncio.run(main())