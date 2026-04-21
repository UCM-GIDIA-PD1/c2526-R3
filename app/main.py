from src.extraccion.minioFunctions import crear_cliente, bajar_modelo
from fastapi import FastAPI
from joblib import load

@asynccontextmanager
async def lifespan(app: FastAPI):
    client = crear_cliente()
    app.state.model = bajar_modelo(client, path_modelo) # Por poner el path ya que no está el modelo todavía en el minio
    yield
