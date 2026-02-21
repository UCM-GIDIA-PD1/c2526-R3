import sys
import os
from pathlib import Path
import asyncio
from dotenv import load_dotenv
import pandas as pd
import traceback

# Sacamos el path actual, su padre y esa será la ruta donde se buscan los otros paquetes
src_path = Path(__file__).parent
sys.path.append(str(src_path))

# Cargas desde el INICIO todas las claves de entorno, por si se llaman
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
    print("\n📦 Importando módulos de extraccion...")

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
    from extraccion import vegetacion2
    print("   ✅ OK")

    print("   puntos_sinteticos")
    from extraccion import puntos_sinteticos
    print("   ✅ OK")

    MODULOS_CARGADOS = True
    print("\n✅ Todos los módulos posibles se cargaron correctamente.\n")

except Exception as e:
    print(f"\n Error crítico al importar módulos: {e}")
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

    limit_input = input("limit (número entero, dejar vacío para 20): ").strip()
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
    limpiar_pantalla()
    print("\n" + "-"*60)
    print("  SISTEMA DE ANÁLISIS DE INCENDIOS ")
    print("-"*60)

    ruta_creds = os.getenv('RUTA_CREDENCIALES', 'No definida')
    ruta_incendios = os.getenv('INCENDIOS', 'No definida')
    print(f"\n📁 RUTA_CREDENCIALES: {formatear_ruta(ruta_creds)}")
    print(f"  INCENDIOS: {formatear_ruta(ruta_incendios)}")
    print(f"  Earth Engine: {'✅ OK' if EE_OK else '❌ Error'}")
    print(f"  Módulos base: {'✅ Cargados' if MODULOS_CARGADOS else '❌ No disponibles'}")
    print(" "*60)

    print("\n📋 MENÚ PRINCIPAL:")
    if MODULOS_CARGADOS:
        print("  1. Construcción DF Ambiental (parámetros: limit, fechas)")
        print("  2. Vegetación (parámetros: limit, fechas)")
        print("  3. Pendiente (parámetros: limit, fechas)")
        print("  4. Características Físicas (parámetros: limit, fechas)")
        print("  5. Vegetación 2 (parámetros: limit, fechas)")
        print("  6. Información del Proyecto")
        print("  7. Diagnosticar Sistema")
        print("  8. Verificar archivo INCENDIOS")
        print("  9. Generar puntos sintéticos (requiere archivo Parquet)")
        print("  0. Salir")
    else:
        print("  ⚠️  Módulos no disponibles (ejecuta opción 7 para diagnosticar)")
        print("  7. Diagnosticar Sistema")
        print("  8. Verificar archivo INCENDIOS")
        print("  0. Salir")
    print(" "*60)

async def diagnosticar_sistema():
    print("\n🔍 DIAGNÓSTICO COMPLETO")
    print("="*50)

    print(f"Python: {sys.version}")
    print(f"Directorio actual: {os.getcwd()}")
    print(f"src path: {src_path}")

    print("\n📁 Variables de entorno (.env):")
    ruta_creds = os.getenv('RUTA_CREDENCIALES')
    ruta_incendios = os.getenv('INCENDIOS')
    print(f"   RUTA_CREDENCIALES: {'✅' if ruta_creds else '❌'} {ruta_creds}")
    print(f"   INCENDIOS: {'✅' if ruta_incendios else '❌'} {ruta_incendios}")

    if ruta_creds:
        print(f"\n📁 Verificando RUTA_CREDENCIALES:")
        if os.path.exists(ruta_creds):
            print(f"      ✅ Existe")
            if os.path.isfile(ruta_creds):
                print(f"      📄 Es archivo")
            else:
                print(f"      📁 Es directorio")
                json_files = list(Path(ruta_creds).glob('*.json'))
                if json_files:
                    print(f"      Archivos JSON encontrados: {[f.name for f in json_files]}")
                else:
                    print(f"      ⚠️ No hay archivos .json")
        else:
            print(f"      ❌ No existe")

    if ruta_incendios:
        print(f"\n📁 Verificando INCENDIOS:")
        if os.path.exists(ruta_incendios):
            print(f"      ✅ Existe")
            if os.path.isfile(ruta_incendios):
                print(f"      📄 Es archivo")
                tam = os.path.getsize(ruta_incendios)
                print(f"      Tamaño: {tam} bytes ({tam/1024/1024:.2f} MB)")
                if ruta_incendios.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(ruta_incendios, nrows=2)
                        print(f"      ✅ CSV legible, columnas: {list(df.columns)}")
                    except Exception as e:
                        print(f"      ❌ Error al leer CSV: {e}")
                elif ruta_incendios.lower().endswith('.parquet'):
                    try:
                        df = pd.read_parquet(ruta_incendios, columns=['lat_mean','lon_mean','date_first','frp_mean'])
                        print(f"      ✅ Parquet legible, columnas requeridas presentes")
                    except Exception as e:
                        print(f"      ❌ Error al leer Parquet: {e}")
            else:
                print(f"      📁 Es directorio")
        else:
            print(f"      ❌ No existe")

    print(f"\n📦 Módulos de Python instalados:")
    modulos = [
        ('ee', 'earthengine-api'),
        ('pandas', 'pandas'),
        ('numpy', 'numpy'),
        ('dotenv', 'python-dotenv'),
        ('aiohttp', 'aiohttp'),
        ('geopandas', 'geopandas'),
        ('rasterio', 'rasterio'),
        ('pyproj', 'pyproj'),
        ('shapely', 'shapely')
    ]
    for mod, pip in modulos:
        try:
            __import__(mod)
            print(f"   ✅ {mod}")
        except ImportError:
            print(f"   ❌ {mod} (pip install {pip})")

    print(f"\n   Earth Engine inicializado: {'✅ Sí' if EE_OK else '❌ No'}")

async def verificar_archivo_incendios():
    """Opción 8: ver detalles del archivo de incendios"""
    print("\n📂 VERIFICACIÓN DETALLADA DEL ARCHIVO INCENDIOS")
    print("="*50)
    ruta = os.getenv('INCENDIOS')
    if not ruta:
        print("❌ Variable INCENDIOS no definida")
        return

    if not os.path.exists(ruta):
        print(f"❌ El archivo no existe: {ruta}")
        return

    if not os.path.isfile(ruta):
        print(f"❌ No es un archivo: {ruta}")
        return

    print(f"📄 Archivo: {ruta}")
    print(f"📏 Tamaño: {os.path.getsize(ruta):,} bytes")
    print(f"📁 Extensión: {Path(ruta).suffix}")

    if ruta.lower().endswith('.csv'):
        try:
            df = pd.read_csv(ruta)
            print(f"\n CSV cargado. {len(df)} filas, columnas: {list(df.columns)}")
            print("\nPrimeras 5 filas:")
            print(df.head())
        except Exception as e:
            print(f" Error al leer CSV: {e}")
    elif ruta.lower().endswith('.parquet'):
        try:
            df = pd.read_parquet(ruta)
            print(f"\n Parquet cargado. {len(df)} filas, columnas: {list(df.columns)}")
            print("\nPrimeras 5 filas:")
            print(df.head())
        except Exception as e:
            print(f" Error al leer Parquet: {e}")
    else:
        print(" Formato no reconocido (solo .csv o .parquet)")

async def ejecutar_funcion(nombre, func, *args, **kwargs):
    print(f"▶️ Ejecutando: {nombre}")
    try:
        resultado = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
        print(f" {nombre} completada.")
        return resultado
    except Exception as e:
        print(f" Error en {nombre}: {e}")
        traceback.print_exc()

def obtener_lista_ficheros():
    entrada = input("Introduce los paths separados por espacios: ")
    datos = entrada.split()
    lista = list(datos)
    return lista
    
# MAIN
async def main():
    while True:
        await mostrar_menu()
        opcion = input("\n🔷 Selecciona una opción: ").strip()

        if opcion == "1" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Construcción DF Ambiental", construccion_df.build_environmental_df, os.getenv('INCENDIOS'))
            else:
                await ejecutar_funcion("Construcción DF Ambiental", construccion_df.build_environmental_df, 
                                       os.getenv('INCENDIOS'), limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "2" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Vegetación", vegetacion.df_vegetacion, os.getenv('INCENDIOS'))
            else:
                await ejecutar_funcion("Vegetación", vegetacion.df_vegetacion, 
                                       os.getenv('INCENDIOS'), limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "3" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Pendiente", pendiente.df_pendiente, os.getenv('INCENDIOS'))
            else:
                await ejecutar_funcion("Pendiente", pendiente.df_pendiente, 
                                       os.getenv('INCENDIOS'), limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "4" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Características Físicas", fisicas.df_fisicas, os.getenv('INCENDIOS'))
            else:
                await ejecutar_funcion("Características Físicas", fisicas.df_fisicas, 
                                       os.getenv('INCENDIOS'), limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "5" and MODULOS_CARGADOS:
            limit, fecha_ini, fecha_fin = obtener_parametros()
            if limit is None:
                await ejecutar_funcion("Vegetación 2", vegetacion2.df_vegetacion2, os.getenv('INCENDIOS'))
            else:
                await ejecutar_funcion("Vegetación 2", vegetacion2.df_vegetacion2, 
                                       os.getenv('INCENDIOS'), limit=limit, fecha_ini=fecha_ini, fecha_fin=fecha_fin)

        elif opcion == "6":
            print("\n" + " "*60)
            print("📋 INFORMACIÓN DEL PROYECTO")
            print(" "*60)
            print("Este sistema permite extraer variables ambientales para incendios.")
            print("Utiliza datos de Earth Engine y Open-Meteo.")
            print("Para más información, consulta la documentación.")
            print("="*60)

        elif opcion == "7":
            await diagnosticar_sistema()

        elif opcion == "8":
            await verificar_archivo_incendios()

<<<<<<< HEAD
        elif opcion == "9" and MODULOS_CARGADOS:
            # Opción 9: Generar puntos sintéticos
            ruta_parquet = input("Ruta del archivo Parquet con incendios (vacío para usar RUTA_PRUEBA de .env): ").strip()
            if not ruta_parquet:
                ruta_parquet = os.getenv('RUTA_PRUEBA')
                if not ruta_parquet:
                    print("No se definió RUTA_PRUEBA en .env ni se proporcionó ruta.")
                    input("\n⏎ Presiona Enter para continuar...")
                    continue

            if not os.path.exists(ruta_parquet):
                print(f"❌ El archivo no existe: {ruta_parquet}")
                input("\n⏎ Presiona Enter para continuar...")
                continue

            if ruta_parquet.lower().endswith('.csv'):
                print("⚠️ El archivo proporcionado es CSV, pero se necesita Parquet.")
                convertir = input("¿Convertir a Parquet temporalmente? (s/n): ").strip().lower()
                if convertir == 's':
                    try:
                        print("Leyendo CSV...")
                        df_csv = pd.read_csv(ruta_parquet)
                        ruta_parquet_temp = "resumen_incendios.parquet"
                        df_csv.to_parquet(ruta_parquet_temp)
                        ruta_parquet = ruta_parquet_temp
                        print(f" BIEN: Convertido a {ruta_parquet_temp}")
                    except Exception as e:
                        print(f" Error al convertir: {e}")
                        input("\n⏎ Presiona Enter para continuar...")
                        continue
                else:
                    print("❌ No se puede continuar sin un archivo Parquet.")
                    input("\n⏎ Presiona Enter para continuar...")
                    continue
            elif not ruta_parquet.lower().endswith('.parquet'):
                print("❌ El archivo debe tener extensión .parquet")
                input("\n⏎ Presiona Enter para continuar...")
                continue

            print(f"\n📊 Generando puntos sintéticos a partir de: {ruta_parquet}")
            try:
                
                # Es un hilo separado para no molestar la sincronización

                df_resultado = await asyncio.to_thread(puntos_sinteticos.crearSinteticos, ruta_parquet, None, None)
                print(f"\n✅ Se generaron {len(df_resultado)} puntos sintéticos.")
                print("\nPrimeras 10 filas:")
                print(df_resultado.head(10))
                
                guardar = input("\n¿Guardar resultado en CSV? (s/n): ").strip().lower()
                if guardar == 's':
                    nombre_csv = input("Nombre del archivo CSV (vacío para 'sinteticos.csv'): ").strip()
                    if not nombre_csv:
                        nombre_csv = "sinteticos.csv"
                    df_resultado.to_csv(nombre_csv, index=False)
                    print(f"✅ Guardado en {nombre_csv}")
            except Exception as e:
                print(f"   Error durante la generación: {e}")
                traceback.print_exc()
=======
        elif opcion == "9":
            print(f"Mergear ficheros: ")
            lista = obtener_lista_ficheros()
            df = construccion_df.merge_parquets(lista)
            print("Merge correcto")
            print(df)
>>>>>>> rama-Ignacio

        elif opcion == "0":
            print("\n👋 ¡Adiós! Pasa un buen día.")
            break

        else:
            print("\n   Opción no válida o módulos no disponibles.")

        input("\n⏎ Presiona Enter para continuar...")

if __name__ == "__main__":
    asyncio.run(main())