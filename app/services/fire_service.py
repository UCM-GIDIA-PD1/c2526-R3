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

async def extraer_variables_punto(lat: float, lon: float, fecha_str: str) -> tuple[dict, int]:
    """
    Extrae todas las variables requeridas para un punto en una fecha dada.
    Si hay nulos, lo reintenta hasta 3 veces.
    Retorna el diccionario de features y los datos faltantes.
    """
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
    
    dias = pd.to_datetime(fecha_str).dayofyear
    features['dia_sin'] = np.sin(2 * np.pi * dias / 365)
    features['dia_cos'] = np.cos(2 * np.pi * dias / 365)

    df_temp = pd.DataFrame([{'lat': lat, 'lon': lon, 'date': fecha_str}])
    
    intentos = 0
    while intentos < 3:
        try:
            # Físicas
            async with aiohttp.ClientSession() as session:
                fisicas_data = await fisicas.fetch_environment(session, lat, lon, fecha_str, directo=True)
            
            # Vegetación
            veg_data = await vegetacion.vegetacion(lat, lon, fecha_str)
            
            # Pendiente
            pen_data = await pendiente.pendiente(lat, lon, fecha_str)
            
            # Suelo
            suelo_list = await suelo2.soil_temp(lat, lon, fecha_str, 0)
            suelo_data = suelo_list[0] if suelo_list and len(suelo_list) > 0 else {}
            
            cliente = await asyncio.to_thread(minioFunctions.crear_cliente)
            path = 'grupo3/maps/civilizaciones/poblaciones_clean.parquet'
            df_pobl = await asyncio.to_thread(minioFunctions.bajar_fichero, cliente, path)
            # Civilización
            distancias = await asyncio.to_thread(civilizacion.calcular_distancias, df_pobl, df_temp)
            dist_civ = distancias.flatten()[0] if distancias is not None else np.nan
            
            features.update(fisicas_data)
            features.update(veg_data)
            features.update(pen_data)
            features.update({
                'soil_temp': suelo_data.get('soil_temp', np.nan),
                'dist_civ': dist_civ
            })
            
            faltantes = sum(pd.isna(v) or v is None for v in features.values())
            if faltantes == 0:
                break
                
        except Exception as e:
            print(f"Error en intento {intentos + 1}: {e}")
            
        intentos += 1
        await asyncio.sleep(1) # Pausa entre intentos
        
    faltantes = sum(pd.isna(v) or v is None for v in features.values())
    return features, faltantes

def realizar_inferencia_ocurrencia(modelo_ocurrencia, features: dict) -> tuple[float, bool]:
    if not modelo_ocurrencia:
        return 0.5, True
        
    try:
        df_predict = pd.DataFrame([features])
        columnas_ocurrencia = [
            'lat', 'lon', 'soil_temp', 'final', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
        
        df_predict_ocurrencia = df_predict[columnas_ocurrencia]
        proba = float(modelo_ocurrencia.predict_proba(df_predict_ocurrencia)[0][1])
        ocurrencia = proba > 0.5
        
        return proba, ocurrencia
    except Exception as e:
        print(f"Error en predicción de ocurrencia: {e}")
        return 0.0, False

def realizar_inferencia_intensidad(modelo_frp, features: dict) -> float:
    if not modelo_frp:
        return 0.0
        
    try:
        df_predict = pd.DataFrame([features])

        # Sin la columna 'final'
        columnas_frp = [
            'lat', 'lon', 'soil_temp', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
        df_predict_frp = df_predict[columnas_frp]
        intensidad = float(modelo_frp.predict(df_predict_frp)[0])
        
        return max(0.0, intensidad)
    except Exception as e:
        print(f"Error en predicción de intensidad: {e}")
        return 0.0

async def procesar_ocurrencia(request: IncendioRequest, modelo_ocurrencia) -> dict:
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
        feat_names = [
            'lat', 'lon', 'date', 'soil_temp', 'final', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
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
        feat_names = [
            'lat', 'lon', 'date', 'soil_temp', 'elevacion_centro',
            'grados', 'porcentaje', 'temp_mean', 'temp_max', 'temp_min',
            'humidity_mean', 'precipitation', 'wind_speed_max', 'wind_gusts_max',
            'pressure_mean', 'cloud_cover', 'radiation', 'evapotranspiration',
            'sunshine_seconds', 'NDVI', 'NDWI', 'dist_civ', 'dia_sin', 'dia_cos'
        ]
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
