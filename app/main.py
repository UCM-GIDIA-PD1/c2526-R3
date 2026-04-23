from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
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
        path_modelo_frp = "grupo3/Modelos/modelo_xgboost_frp.pkl" 
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
