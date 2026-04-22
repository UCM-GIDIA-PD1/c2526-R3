from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.extraccion.minioFunctions import crear_cliente, bajar_modelo, bajar_fichero, bajar_imagen
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
        path_modelo_frp = "grupo3/Modelos/modelo_xgboost_frp.pkl" 
        ml_models["xgboost_frp"] = bajar_modelo(cliente, path_modelo_frp)
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
