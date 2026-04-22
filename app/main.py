from extraccion.minioFunctions import crear_cliente, bajar_modelo, bajar_imagen

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import Annotated
from pydantic import BaseModel
from joblib import load


# Se ejecuta una vez al arrancar el servidor (startup) y al apagarlo (shutdown)
@asynccontextmanager
async def lifespan(app: FastAPI):
    cliente = crear_cliente()
    #app.state.model = bajar_modelo(cliente, path_modelo) # Por poner el path ya que no está el modelo todavía en el minio

    yield

# Creamos la aplicación web
app = FastAPI(lifespan=lifespan)


@app.get("/imagen/{filename}")
def obtener_imagen_minio(filename: str):
    """
    Endpoint para descargar imágenes en tiempo real desde MinIO.
    """

    try:
        # Bajamos las fotos
        imagen = bajar_imagen(crear_cliente(), f"grupo3/imagenes/{filename}")
        
        tipo = "image/png" if filename.endswith(".png") else "image/jpeg"

        # FastAPI envía todo el contenido sin subirlo todo a la RAM
        return StreamingResponse(imagen.stream(32*1024), media_type=tipo)
    
    except Exception as e:
        print(f"Error cargando imagen de MinIO: {e}")
        return {"error": "Imagen no encontrada"}