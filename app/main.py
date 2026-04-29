from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import asyncio
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.extraccion.minioFunctions import crear_cliente, bajar_fichero, bajar_imagen
from app.schemas import IncendioRequest, OcurrenciaResponse, IntensidadResponse
from app.services.fire_service import procesar_ocurrencia, procesar_intensidad

ml_models = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        print("Cargando modelo de ocurrencia desde MinIO...")
        cliente = crear_cliente()
        path_modelo_ocurrencia = "grupo3/modelo_xgboost_final.pkl"
        ml_models["xgboost_ocurrencia"] = bajar_fichero(cliente, path_modelo_ocurrencia, type="pkl")
        print("Modelo de ocurrencia cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el modelo de ocurrencia desde MinIO: {e}")
        ml_models["xgboost_ocurrencia"] = None

    try:
        print("Cargando modelo de intensidad (FRP) desde MinIO...")
        cliente = crear_cliente()
        path_modelo_frp = "grupo3/modelo_xgboost_frp.pkl" 
        ml_models["xgboost_frp"] = bajar_fichero(cliente, path_modelo_frp, type="pkl")
        print("Modelo de intensidad cargado exitosamente.")
    except Exception as e:
        print(f"Error al cargar el modelo de intensidad (FRP) desde MinIO: {e}")
        ml_models["xgboost_frp"] = None
        
    yield
    
    ml_models.clear()

app = FastAPI(
    title="API de Predicción de Incendios",
    description="API REST profesional para el sistema de predicción de incendios forestales usando XGBoost.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

template_index = Jinja2Templates(directory="app/mapa-ignis/dist")
app.mount("/assets", StaticFiles(directory="app/mapa-ignis/dist/assets"), name="assets")

# Endpoint para la página principal
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    """
    Endpoint para mostrar la página principal
    """
    return template_index.TemplateResponse(request=request, name="index.html")


templates = Jinja2Templates(directory="app/templates")

# Endpoints para las páginas de información 
@app.get("/info_incendios", response_class=HTMLResponse)
def info_incendios(request: Request):
    """
    Endpoint para mostrar la página informativa del modelo de incendios
    """
    return templates.TemplateResponse(request=request, name="info_incendios.html")

@app.get("/info_frp", response_class=HTMLResponse)
def info_frp(request: Request):
    """
    Endpoint para mostrar la página informativa del modelo de frp
    """
    return templates.TemplateResponse(request=request, name="info_frp.html")

# Endpoint para obtener las imágenes desde MinIO
@app.get("/imagen/{filename}")
def obtener_imagen_minio(filename: str):
    """
    Endpoint para descargar imágenes en tiempo real desde MinIO.
    """
    try:
        cliente = crear_cliente()
        imagen = bajar_imagen(cliente, f"grupo3/imagenes/{filename}")
        tipo = "image/png" if filename.endswith(".png") else "image/jpeg"
        return StreamingResponse(imagen.stream(32*1024), media_type=tipo)
    except Exception as e:
        print(f"Error cargando imagen de MinIO: {e}")
        return {"error": "Imagen no encontrada"}

# Endpoints para las predicciones
@app.post("/predict/ocurrencia", response_model=OcurrenciaResponse)
async def predict_ocurrencia(request: IncendioRequest):
    """
    Endpoint para predecir si habrá un incendio o no basado en variables espaciales y meteorológicas.
    """
    modelo_ocurrencia = ml_models.get("xgboost_ocurrencia")
    response_data = await procesar_ocurrencia(request, modelo_ocurrencia)
    return response_data

@app.post("/predict/intensidad", response_model=IntensidadResponse)
async def predict_intensidad(request: IncendioRequest):
    """
    Endpoint para predecir la intensidad teórica (FRP) de un incendio en una ubicación y fecha.
    """
    modelo_frp = ml_models.get("xgboost_frp")
    response_data = await procesar_intensidad(request, modelo_frp)
    return response_data

@app.get("/{filename}")
def get_root_static(filename: str):
    """
    Endpoint Catch-All para servir archivos en la raíz del build (como fires.geojson, ico, png, etc)
    """
    file_path = os.path.join("app/mapa-ignis/dist", filename)
    if os.path.isfile(file_path):
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.get("/geojson/{year}")
def obtener_geojson_anio(year: int):
    """
    Endpoint para transmitir archivos GeoJSON desde MinIO según el año
    """
    try:
        cliente = crear_cliente()
        bucket_name = "pd1"
        object_name = f"grupo3/cleaned/geojsons/fires_{year}.geojson" 

        response = cliente.get_object(bucket_name, object_name)
        
        return StreamingResponse(
            response.stream(32*1024), 
            media_type="application/geo+json",
            headers={"Content-Disposition": f"attachment; filename=fires_{year}.geojson"}
        )
        
    except Exception as e:
        print(f"Error al recuperar GeoJSON para el año {year}: {e}")
        raise HTTPException(
            status_code=404, 
            detail=f"No se encontró el archivo GeoJSON para el año {year}."
        )


# ── SSE helpers ─────────────────────────────────────────────────────────────
def sse_event(event: str, data: dict) -> str:
    """Format a single Server-Sent Event message."""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def stream_prediction(request_body: IncendioRequest, tipo: str, modelo):
    """
    Async generator that yields SSE progress events while extracting features,
    then yields the final prediction result.
    """
    from app.services.fire_service import (
        extraer_variables_punto,
        realizar_inferencia_ocurrencia,
        realizar_inferencia_intensidad,
        get_model_features,
    )
    from datetime import date, datetime
    import numpy as np
    import pandas as pd

    hoy = date.today()
    try:
        fecha_obj = datetime.strptime(request_body.fecha, "%Y-%m-%d").date() if request_body.fecha else hoy
    except ValueError:
        fecha_obj = hoy

    if fecha_obj < hoy:
        error_payload = {"error": "Solo se permiten predicciones para hoy o para el futuro (hasta 15 días)."}
        if tipo == "ocurrencia":
            error_payload.update({"ocurrencia": False, "probabilidad": 0.0,
                                   "fecha_procesada": str(fecha_obj), "modelo_version": "XGBoost-Ocurrencia"})
        else:
            error_payload.update({"intensidad": 0.0,
                                   "fecha_procesada": str(fecha_obj), "modelo_version": "XGBoost-FRP"})
        yield sse_event("result", error_payload)
        return

    fecha_procesada = fecha_obj.strftime("%Y-%m-%d")

    # Queue for progress messages produced inside the async callback
    progress_queue: asyncio.Queue = asyncio.Queue()

    async def progress_cb(step: int, total: int, label: str):
        await progress_queue.put({"step": step, "total": total, "label": label})

    # Run extraction in a task so we can drain the queue concurrently
    extraction_task = asyncio.create_task(
        extraer_variables_punto(request_body.latitud, request_body.longitud, fecha_procesada, progress_cb)
    )

    # Stream progress events until extraction finishes
    while not extraction_task.done():
        try:
            msg = await asyncio.wait_for(progress_queue.get(), timeout=0.2)
            yield sse_event("progress", msg)
        except asyncio.TimeoutError:
            pass  # keep looping

    # Drain any remaining messages
    while not progress_queue.empty():
        msg = progress_queue.get_nowait()
        yield sse_event("progress", msg)

    features_completas, datos_faltantes = extraction_task.result()

    # Step 6 — running model
    yield sse_event("progress", {"step": 6, "total": 6, "label": "Ejecutando modelo de predicción..."})

    if datos_faltantes >= 5:
        error_msg = "No se pudieron extraer los datos suficientes. Faltan más de 5 valores."
        if tipo == "ocurrencia":
            yield sse_event("result", {"ocurrencia": False, "probabilidad": 0.0,
                                        "fecha_procesada": fecha_procesada, "modelo_version": "XGBoost-Ocurrencia",
                                        "error": error_msg})
        else:
            yield sse_event("result", {"intensidad": 0.0, "fecha_procesada": fecha_procesada,
                                        "modelo_version": "XGBoost-FRP", "error": error_msg})
        return

    nota = (f"No se extrajeron todos los datos. Faltan {datos_faltantes} valores."
            if 0 < datos_faltantes < 5 else None)

    # ── Build result payload identical to the normal endpoints ────────────
    name_map = {
        'temp_max': 'Temperatura Max', 'humidity_mean': 'Humedad',
        'NDVI': 'Vegetación (NDVI)', 'wind_speed_max': 'Viento',
        'radiation': 'Radiación Solar', 'soil_temp': 'Temp. Suelo',
        'grados': 'Pendiente', 'dist_civ': 'Dist. Civilización',
    }

    if tipo == "ocurrencia":
        probabilidad, ocurrencia = realizar_inferencia_ocurrencia(modelo, features_completas)
        variables_clave = {
            "Temperatura": f"{features_completas.get('temp_max', 0):.1f} °C",
            "Humedad": f"{features_completas.get('humidity_mean', 0):.1f} %",
            "Viento": f"{features_completas.get('wind_speed_max', 0):.1f} km/h",
            "NDVI (Vegetación)": f"{features_completas.get('NDVI', 0):.3f}",
            "NDWI (Agua)": f"{features_completas.get('NDWI', 0):.3f}",
            "Pendiente": f"{features_completas.get('grados', 0):.1f}°"
        }
        importancias = {}
        if modelo and hasattr(modelo, 'feature_importances_'):
            fallback = ['lat','lon','date','soil_temp','final','elevacion_centro','grados','porcentaje',
                        'temp_mean','temp_max','temp_min','humidity_mean','precipitation','wind_speed_max',
                        'wind_gusts_max','pressure_mean','cloud_cover','radiation','evapotranspiration',
                        'sunshine_seconds','NDVI','NDWI','dist_civ','dia_sin','dia_cos']
            feat_names = get_model_features(modelo, fallback)
            imps = modelo.feature_importances_
            for i in np.argsort(imps)[::-1][:5]:
                importancias[name_map.get(feat_names[i], feat_names[i])] = float(imps[i])
        payload = {"ocurrencia": ocurrencia, "probabilidad": probabilidad,
                   "fecha_procesada": fecha_procesada, "modelo_version": "XGBoost-Ocurrencia",
                   "variables_clave": variables_clave, "importancias": importancias}
    else:
        intensidad = realizar_inferencia_intensidad(modelo, features_completas)
        variables_clave = {
            "Temperatura": f"{features_completas.get('temp_max', 0):.1f} °C",
            "Humedad": f"{features_completas.get('humidity_mean', 0):.1f} %",
            "NDVI (Vegetación)": f"{features_completas.get('NDVI', 0):.3f}",
            "Radiación": f"{features_completas.get('radiation', 0):.1f} J/m²"
        }
        importancias = {}
        if modelo and hasattr(modelo, 'feature_importances_'):
            fallback = ['lat','lon','soil_temp','elevacion_centro','grados','porcentaje','temp_mean',
                        'temp_max','temp_min','humidity_mean','precipitation','wind_speed_max',
                        'wind_gusts_max','pressure_mean','cloud_cover','radiation','evapotranspiration',
                        'sunshine_seconds','NDVI','NDWI','dist_civ','dry_fuel_index','VPD','fuel_stress']
            feat_names = get_model_features(modelo, fallback)
            imps = modelo.feature_importances_
            for i in np.argsort(imps)[::-1][:5]:
                importancias[name_map.get(feat_names[i], feat_names[i])] = float(imps[i])
        payload = {"intensidad": intensidad, "fecha_procesada": fecha_procesada,
                   "modelo_version": "XGBoost-FRP", "variables_clave": variables_clave,
                   "importancias": importancias}

    if nota:
        payload["nota_informativa"] = nota

    yield sse_event("result", payload)


@app.post("/predict/ocurrencia/stream")
async def stream_ocurrencia(request: IncendioRequest):
    """
    SSE endpoint: emite eventos de progreso en tiempo real durante la extracción
    y finalmente el resultado de predicción de ocurrencia.
    """
    modelo = ml_models.get("xgboost_ocurrencia")
    return StreamingResponse(
        stream_prediction(request, "ocurrencia", modelo),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/predict/intensidad/stream")
async def stream_intensidad(request: IncendioRequest):
    """
    SSE endpoint: emite eventos de progreso en tiempo real durante la extracción
    y finalmente el resultado de predicción de intensidad (FRP).
    """
    modelo = ml_models.get("xgboost_frp")
    return StreamingResponse(
        stream_prediction(request, "intensidad", modelo),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
