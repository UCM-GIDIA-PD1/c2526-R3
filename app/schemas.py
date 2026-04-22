from pydantic import BaseModel, Field
from typing import Optional

class IncendioRequest(BaseModel):
    latitud: float = Field(..., description="Latitud del punto a predecir")
    longitud: float = Field(..., description="Longitud del punto a predecir")
    fecha: Optional[str] = Field(None, description="Fecha de predicción en formato YYYY-MM-DD")

class OcurrenciaResponse(BaseModel):
    ocurrencia: bool = Field(..., description="Si habrá incendio o no")
    probabilidad: float = Field(..., description="Probabilidad de incendio calculada por el modelo XGBoost")
    fecha_procesada: str = Field(..., description="Fecha final validada y utilizada para la predicción")
    modelo_version: str = Field(..., description="Versión del modelo utilizado")
    variables_clave: Optional[dict] = Field(None, description="Valores de las variables más significativas")
    importancias: Optional[dict] = Field(None, description="Importancia relativa de las variables en el modelo")
    error: Optional[str] = Field(None, description="Mensaje de error si la predicción no fue posible")
    nota_informativa: Optional[str] = Field(None, description="Avisos sobre la extracción de datos")

class IntensidadResponse(BaseModel):
    intensidad: float = Field(..., description="Intensidad esperada del incendio (FRP)")
    fecha_procesada: str = Field(..., description="Fecha final validada y utilizada para la predicción")
    modelo_version: str = Field(..., description="Versión del modelo utilizado")
    variables_clave: Optional[dict] = Field(None, description="Valores de las variables más significativas")
    importancias: Optional[dict] = Field(None, description="Importancia relativa de las variables en el modelo")
    error: Optional[str] = Field(None, description="Mensaje de error si la predicción no fue posible")
    nota_informativa: Optional[str] = Field(None, description="Avisos sobre la extracción de datos")
