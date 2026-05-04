from datetime import date, datetime
import aiohttp
import pandas as pd
import numpy as np
import sys
import os
import asyncio

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.extraccion import vegetacion, pendiente, fisicas
from src.extraccion.futuro import suelo2, civilizacion
from src.extraccion import minioFunctions

from app.schemas import IncendioRequest

async def extraer_variables_punto(
    lat: float,
    lon: float,
    fecha_str: str,
    progress_cb=None   # async callable(step: int, total: int, label: str)
) -> tuple[dict, int]:
    """
    Extrae todas las variables requeridas para un punto en una fecha dada.
    Se hacen de forma paralela para mayor velocidad.

    :params lat: Latitud del punto
    :params lon: Longitud del punto
    :params fecha_str: Fecha en formato string (se tomarán los primeros 10 caracteres)
    :params progress_cb: Callback async opcional para emitir progreso (step, total, label)
    :return tuple[dict, int]: Diccionario con las variables extraídas y el número de valores faltantes
    """
    TOTAL_STEPS = 6

    async def _emit(step: int, label: str):
        if progress_cb:
            await progress_cb(step, TOTAL_STEPS, label)

    features_esperadas = [
        'lat', 'lon', 'date', 'soil_temp', 'final', 'elevacion_centro',
        'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
        'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
        'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
        'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
    ]
    
    features = {k: np.nan for k in features_esperadas}
    features.update({
        'lat': lat,
        'lon': lon,
        'date': fecha_str,
        'final': 0
    })
    
    await _emit(1, "Inicializando variables geoespaciales...")

    dias = pd.to_datetime(fecha_str).dayofyear
    features['dia_sin'] = np.sin(2 * np.pi * dias / 365)
    features['dia_cos'] = np.cos(2 * np.pi * dias / 365)

    df_temp = pd.DataFrame([{'lat': lat, 'lon': lon, 'date': fecha_str}])

    await _emit(2, "Conectando a MinIO y cargando datos...")
    cliente = await asyncio.to_thread(minioFunctions.crear_cliente)
    path_pobl = 'grupo3/maps/civilizaciones/poblaciones_clean.parquet'
    df_pobl = await asyncio.to_thread(minioFunctions.bajar_fichero, cliente, path_pobl)

    await _emit(3, "Calculando distancia a núcleos de población...")
    distancias = await asyncio.to_thread(civilizacion.calcular_distancias, df_pobl, df_temp)
    dist_civ = distancias.flatten()[0] if distancias is not None else np.nan

    await _emit(4, "Extrayendo datos climáticos, vegetación y terreno...")
    intentos = 0
    async with aiohttp.ClientSession() as session:
        while intentos < 3:
            try:
                tareas = [
                    fisicas.fetch_environment(session, lat, lon, fecha_str, directo=True),
                    vegetacion.vegetacion(lat, lon, fecha_str),
                    pendiente.pendiente(lat, lon, fecha_str),
                    suelo2.soil_temp(lat, lon, fecha_str, 0)
                ]
                
                resultados = await asyncio.gather(*tareas, return_exceptions=True)
                
                if not isinstance(resultados[0], Exception): features.update(resultados[0])
                if not isinstance(resultados[1], Exception): features.update(resultados[1])
                if not isinstance(resultados[2], Exception): features.update(resultados[2])
                
                if not isinstance(resultados[3], Exception) and len(resultados[3]) > 0:
                    features['soil_temp'] = resultados[3][0].get('soil_temp', np.nan)
                
                features['dist_civ'] = dist_civ
                
                faltantes = sum(pd.isna(v) or v is None for v in features.values())
                if faltantes <= 1:
                    break
                    
            except Exception as e:
                print(f"Error en intento {intentos + 1}: {e}")
                
            intentos += 1
            if intentos < 3: await asyncio.sleep(0.5)

    await _emit(5, "Validando datos extraídos...")
    faltantes = sum(pd.isna(v) or v is None for v in features.values())
    return features, faltantes

def get_model_features(modelo, fallback_features):
    """
    Obtiene los nombres de las características que espera el modelo.

    :param modelo: El modelo de machine learning (XGBoost, RandomForest, etc.).
    :param fallback_features: Lista de características por defecto en caso de no encontrarlas en el modelo.
    :return: Lista de nombres de características.
    """
    if hasattr(modelo, 'feature_names_in_'):
        return list(modelo.feature_names_in_)
    elif hasattr(modelo, 'feature_names'):
        return list(modelo.feature_names)
    elif hasattr(modelo, 'get_booster'):
        return list(modelo.get_booster().feature_names)
    return fallback_features

def realizar_inferencia_ocurrencia(modelo_ocurrencia, features: dict) -> tuple[float, bool]:
    """
    Realiza la inferencia para predecir la ocurrencia de un incendio.

    :param modelo_ocurrencia: El modelo de clasificación cargado.
    :param features: Diccionario con las variables de entrada.
    :return: Tupla con la probabilidad (float) y si ocurre o no (bool).
    """
    if not modelo_ocurrencia:
        return 0.5, True
        
    try:
        df_predict = pd.DataFrame([features])
        fallback = [
            'lat', 'lon', 'soil_temp', 'final', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
        columnas_ocurrencia = get_model_features(modelo_ocurrencia, fallback)
        
        for col in columnas_ocurrencia:
            if col not in df_predict.columns:
                df_predict[col] = 0.0
                
        df_predict_ocurrencia = df_predict[columnas_ocurrencia]
        proba = float(modelo_ocurrencia.predict_proba(df_predict_ocurrencia)[0][1])
        ocurrencia = proba > 0.5
        
        return proba, ocurrencia
    except Exception as e:
        print(f"Error en predicción de ocurrencia: {e}")
        return 0.0, False

def realizar_inferencia_intensidad(modelo_frp, features: dict) -> float:
    """
    Realiza la inferencia para predecir la intensidad (FRP) de un incendio.

    :param modelo_frp: El modelo de regresión cargado.
    :param features: Diccionario con las variables de entrada.
    :return: Valor de intensidad predicha (FRP).
    """
    if not modelo_frp:
        return 0.0

    try:
        df_predict = pd.DataFrame([features])

        fallback = [
            'lat', 'lon', 'soil_temp', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos',
            'dry_fuel_index', 'VPD', 'fuel_stress'
        ]
        columnas_frp = get_model_features(modelo_frp, fallback)
        
        for col in columnas_frp:
            if col not in df_predict.columns:
                df_predict[col] = 0.0

        df_predict_frp = df_predict[columnas_frp]
        intensidad = float(modelo_frp.predict(df_predict_frp)[0])
         
        return max(0.0, intensidad)
    except Exception as e:
        print(f"Error en predicción de intensidad: {e}")
        return 0.0

async def procesar_ocurrencia(request: IncendioRequest, modelo_ocurrencia) -> dict:
    """
    Orquesta el proceso de extracción de variables y predicción de ocurrencia.

    :param request: Objeto de solicitud con coordenadas y fecha.
    :param modelo_ocurrencia: El modelo de clasificación a utilizar.
    :return: Diccionario con el resultado de la predicción y metadatos.
    """
    hoy = date.today()
    
    if request.fecha:
        try:
            fecha_obj = datetime.strptime(request.fecha, "%Y-%m-%d").date()
        except ValueError:
            fecha_obj = hoy
    else:
        fecha_obj = hoy

    if fecha_obj < hoy:
        return {
            "ocurrencia": False,
            "probabilidad": 0.0,
            "fecha_procesada": str(fecha_obj),
            "modelo_version": "XGBoost-Ocurrencia",
            "error": "Solo se permiten predicciones para hoy o para el futuro (hasta 15 días)."
        }

    fecha_procesada = fecha_obj.strftime("%Y-%m-%d")

    features_completas, datos_faltantes = await extraer_variables_punto(request.latitud, request.longitud, fecha_procesada)
    
    if datos_faltantes >= 5:
        return {
            "ocurrencia": False,
            "probabilidad": 0.0,
            "fecha_procesada": fecha_procesada,
            "modelo_version": "XGBoost-Ocurrencia",
            "error": "No se pudieron extraer los datos suficientes para la predicción correctamente. Faltan más de 5 valores."
        }
    
    nota = f"No se extrajeron todos los datos suficientes. Faltan {datos_faltantes} valores." if 0 < datos_faltantes < 5 else None

    probabilidad, ocurrencia = realizar_inferencia_ocurrencia(modelo_ocurrencia, features_completas)

    variables_clave = {
        "Temperatura": f"{features_completas.get('temp_max', 0):.1f} °C",
        "Humedad": f"{features_completas.get('humidity_mean', 0):.1f} %",
        "Viento": f"{features_completas.get('wind_speed_max', 0):.1f} km/h",
        "NDVI (Vegetación)": f"{features_completas.get('NDVI', 0):.3f}",
        "NDWI (Agua)": f"{features_completas.get('NDWI', 0):.3f}",
        "Pendiente": f"{features_completas.get('grados', 0):.1f}°"
    }

    importancias = {}
    if modelo_ocurrencia and hasattr(modelo_ocurrencia, 'feature_importances_'):
        fallback = [
            'lat', 'lon', 'date', 'soil_temp', 'final', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
        feat_names = get_model_features(modelo_ocurrencia, fallback)
        importances = modelo_ocurrencia.feature_importances_
        sorted_idx = np.argsort(importances)[::-1][:5]
        name_map = {
            'temp_max': 'Temperatura Max',
            'humidity_mean': 'Humedad',
            'NDVI': 'Vegetación (NDVI)',
            'wind_speed_max': 'Viento',
            'radiation': 'Radiación Solar',
            'soil_temp': 'Temp. Suelo',
            'grados': 'Pendiente',
            'dist_civ': 'Dist. Civilización'
        }
        for i in sorted_idx:
            raw_name = feat_names[i]
            display_name = name_map.get(raw_name, raw_name)
            importancias[display_name] = float(importances[i])

    response_dict = {
        "ocurrencia": ocurrencia,
        "probabilidad": probabilidad,
        "fecha_procesada": fecha_procesada,
        "modelo_version": "XGBoost-Ocurrencia",
        "variables_clave": variables_clave,
        "importancias": importancias
    }

    if nota:
        response_dict["nota_informativa"] = nota

    return response_dict


async def procesar_intensidad(request: IncendioRequest, modelo_frp) -> dict:
    """
    Orquesta el proceso de extracción de variables y predicción de intensidad.

    :param request: Objeto de solicitud con coordenadas y fecha.
    :param modelo_frp: El modelo de regresión a utilizar.
    :return: Diccionario con el resultado de la predicción y metadatos.
    """
    hoy = date.today()
    
    if request.fecha:
        try:
            fecha_obj = datetime.strptime(request.fecha, "%Y-%m-%d").date()
        except ValueError:
            fecha_obj = hoy
    else:
        fecha_obj = hoy

    if fecha_obj < hoy:
        return {
            "intensidad": 0.0,
            "fecha_procesada": str(fecha_obj),
            "modelo_version": "XGBoost-FRP",
            "error": "Solo se permiten predicciones para hoy o para el futuro (hasta 15 días)."
        }

    fecha_procesada = fecha_obj.strftime("%Y-%m-%d")

    features_completas, datos_faltantes = await extraer_variables_punto(request.latitud, request.longitud, fecha_procesada)
    if datos_faltantes >= 5:
        return {
            "intensidad": 0.0,
            "fecha_procesada": fecha_procesada,
            "modelo_version": "XGBoost-FRP",
            "error": "No se pudieron extraer los datos suficientes para la predicción correctamente. Faltan más de 5 valores."
        }
    
    nota = f"No se extrajeron todos los datos suficientes. Faltan {datos_faltantes} valores." if 0 < datos_faltantes < 5 else None

    intensidad = realizar_inferencia_intensidad(modelo_frp, features_completas)

    variables_clave = {
        "Temperatura": f"{features_completas.get('temp_max', 0):.1f} °C",
        "Humedad": f"{features_completas.get('humidity_mean', 0):.1f} %",
        "NDVI (Vegetación)": f"{features_completas.get('NDVI', 0):.3f}",
        "Radiación": f"{features_completas.get('radiation', 0):.1f} J/m²"
    }

    importancias = {}
    if modelo_frp and hasattr(modelo_frp, 'feature_importances_'):
        fallback = [
            'lat', 'lon', 'soil_temp', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dry_fuel_index', 'VPD', 'fuel_stress'
        ]
        feat_names = get_model_features(modelo_frp, fallback)
        importances = modelo_frp.feature_importances_
        sorted_idx = np.argsort(importances)[::-1][:5]
        name_map = {
            'temp_max': 'Temperatura Max',
            'humidity_mean': 'Humedad',
            'NDVI': 'Vegetación (NDVI)',
            'radiation': 'Radiación Solar',
            'grados': 'Pendiente'
        }
        for i in sorted_idx:
            raw_name = feat_names[i]
            display_name = name_map.get(raw_name, raw_name)
            importancias[display_name] = float(importances[i])

    response_dict = {
        "intensidad": intensidad,
        "fecha_procesada": fecha_procesada,
        "modelo_version": "XGBoost-FRP",
        "variables_clave": variables_clave,
        "importancias": importancias
    }

    if nota:
        response_dict["nota_informativa"] = nota

    return response_dict
