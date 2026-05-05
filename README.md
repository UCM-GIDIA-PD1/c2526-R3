# IgnisAI

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Status](https://img.shields.io/badge/status-en%20desarrollo-yellow)
![UCM](https://img.shields.io/badge/UCM-Proyecto%20de%20Datos%20I-red)

<img width="2581" height="1245" alt="Gemini_Generated_Image_wr92ufwr92ufwr92" src="https://github.com/user-attachments/assets/55b90c3c-eead-4330-b572-862d93f18a51" />

## Descripción

IgnisAI es un sistema diseñado para predecir la probabilidad de ignición y estimar la intensidad de incendios forestales (Fire Radiative Power, FRP) basándose en variables meteorológicas, topográficas y de vegetación. El objetivo es proporcionar una herramienta de apoyo a la toma de decisiones que permita a diversos actores (administración pública, sector privado, gestores forestales...) anticiparse al riesgo y optimizar la asignación de recursos de emergencia.

---

## Requisitos previos

Antes de comenzar, asegúrate de tener lo siguiente:

- Python >= 3.12 (probado con 3.12.8)
- [`uv`](https://github.com/astral-sh/uv) instalado como gestor de dependencias
- Cuenta activa en [Weights & Biases](https://wandb.ai/) (para registro de experimentos)
- Acceso al servidor MinIO del proyecto
- Cuenta activa en [Google Earth Engine](https://earthengine.google.com/) (solo necesaria para re-ejecutar la extracción de datos)

---

## Estructura del repositorio

```text
c2526-R3/
├── src/
│   ├── extraccion/                     # Scripts de extracción de datos (Fase 2)
│   │   ├── descartadas/                # Variables descartadas del pipeline final
│   │   │   ├── vegetacion2.py
│   │   │   └── suelo.py
│   │   ├── futuro/                     # Ideas y experimentos no integrados aún
│   │   ├── construccion_df.py
│   │   ├── filtros_no_incendio.py
│   │   ├── fisicas.py
│   │   ├── incendios.py
│   │   ├── mascaras.py
│   │   ├── minioFunctions.py
│   │   ├── parquet.py
│   │   ├── pendiente.py
│   │   ├── puntos_no_incendio.py
│   │   └── vegetacion.py
│   ├── analisis/                       # Notebooks de análisis exploratorio
│   │   ├── analisis_problema1.ipynb
│   │   ├── analisis_problema2.ipynb
│   │   └── ...
│   ├── limpieza/                       # Limpieza y transformación del dataset
│   │   ├── limpieza.py
│   │   └── transformacion.py
│   ├── modelos/                        # Entrenamiento y evaluación de modelos (Fase 3)
│   │   ├── baseline/                   # Modelos baseline (regresión logística y lineal)
│   │   │   ├── baseline.ipynb
│   │   │   └── baseline_frp.ipynb
│   │   ├── clasificacion/              # Modelos de clasificación (predicción de incendio)
│   │   │   ├── balanced_random_forest.py
│   │   │   ├── decisiontree.py
│   │   │   ├── m_xgboost.py
│   │   │   ├── random_forest.py
│   │   │   ├── regresion_logistica.py
│   │   │   └── ventanas_temporales.py
│   │   ├── regresion/                  # Modelos de regresión (estimación de FRP)
│   │   │   ├── arboles/
│   │   │   │   ├── frp_rdForest.py
│   │   │   │   └── frp_xgBoost.py
│   │   │   ├── knn/
│   │   │   │   └── Knn_regressor.py
│   │   │   └── lineal/
│   │   │       └── regresion_lineal_sfs.py
│   │   ├── evaluacion/
│   │   │   └── evaluacion_final.py     # Evaluación sobre conjunto test
│   │   └── utils/                      # Utilidades compartidas por todos los modelos
│   │       ├── anomalias.py
│   │       ├── carga_datos.py
│   │       ├── metricas.py
│   │       ├── particiones.py
│   │       ├── personalizacion.py
│   │       └── wandbFunctions.py
│   └── main.py
├── .gitignore
├── .python-version
├── README.md
├── pyproject.toml
└── uv.lock
```

---

## Estructura de los datos en MinIO

```text
pd1/
└── grupo3/
    ├── maps/
    │   ├── Biogeoregiones/ ...        # .shp
    │   ├── mapa/ ...                  # .tif
    │   ├── Incendios_firms/ ...       # .csv
    │   └── SOC/ ...                   # .tif
    ├── raw/
    │   ├── Fisicas/ ...
    │   ├── Incendios_y_no_incendios/ ...
    │   ├── Pendiente/ ...
    │   ├── Vegetacion/ ...
    │   └── ...
    ├── cleaned/
    │   ├── MINI.parquet               # Dataset general: incendios + no incendios (clasificación)
    │   └── MI.parquet                 # Dataset solo incendios (regresión FRP)
    └── grupo.txt
```

---

## Configuración de Weights & Biases

El proyecto utiliza [Weights & Biases](https://wandb.ai/) para el registro de todos los experimentos de entrenamiento. Necesitas una cuenta y una API key.

1. Regístrate en [wandb.ai](https://wandb.ai/) si no tienes cuenta.
2. Ve a [wandb.ai/settings](https://wandb.ai/settings) y copia tu **API key**.
3. Añádela a tu archivo `.env` (ver sección siguiente):

```env
WANDB_KEY=TU_API_KEY
```

Los experimentos se registrarán automáticamente en el team `pd1-c2526-team3` al ejecutar cualquier script de modelado.

---

## Configuración de MinIO

El proyecto cuenta con funciones de conexión con MinIO para subir y bajar ficheros sin necesidad de trabajar en local. Para que esto funcione, añade a tus variables de entorno:

1. `AWS_ACCESS_KEY_ID` → ACCESS KEY de MinIO.
2. `AWS_SECRET_ACCESS_KEY` → SECRET KEY de MinIO.

---

## Configuración del archivo `.env`

Crea un archivo `.env` en la raíz del proyecto (`c2526-R3/`) con la siguiente estructura:

```env
WANDB_KEY=TU_API_KEY_DE_WANDB
AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
VITE_MAPBOX_TOKEN=TOKEN_DE_MAPBOX
RUTA_CREDENCIALES=/ruta/completa/a/google-credentials.json
```

Fijarse en .env.example 

### Descripción de las variables

| Variable | Descripción |
|----------|-------------|
| `WANDB_KEY` | API key de Weights & Biases para registrar experimentos. |
| `AWS_ACCESS_KEY_ID` | Clave de acceso para la conexión con MinIO. |
| `AWS_SECRET_ACCESS_KEY` | Clave secreta asociada a la cuenta de MinIO. |
| `RUTA_CREDENCIALES` | Ruta absoluta al archivo `google-credentials.json` de Google Earth Engine (solo necesaria para re-ejecutar la extracción de datos). |

### Consideraciones de seguridad

- El archivo `.env` **no debe subirse al repositorio**.
- El archivo `google-credentials.json` **no debe versionarse**.
- Ambos están incluidos en `.gitignore`.
- Las credenciales deben tratarse como información confidencial.

---

## Ejecución del proyecto

### 1. Clonar el repositorio

```bash
git clone https://github.com/UCM-GIDIA-PD1/c2526-R3.git
cd c2526-R3
```

### 2. Instalar dependencias

```bash
uv sync
```

Este comando instalará automáticamente todas las dependencias definidas en `pyproject.toml` y fijadas en `uv.lock`, garantizando la reproducibilidad del entorno.

### 3. Entrenar un modelo

Cada script de modelado se ejecuta de forma independiente. Por ejemplo, para entrenar la regresión logística:

```bash
uv run python src/modelos/clasificacion/regresion_logistica.py
```

El script pedirá por consola el método de búsqueda de hiperparámetros (`grid`, `random` o `bayes`) y la métrica a optimizar (`f1` o `f2`). Los resultados quedan registrados automáticamente en Weights & Biases.

### 4. Evaluar el mejor modelo sobre test

```bash
uv run python src/modelos/evaluacion/evaluacion_final.py
```

### 5. Verificación de correcto funcionamiento

El sistema se considera correctamente configurado si:

- No aparecen errores de autenticación con Weights & Biases.
- No aparecen errores de conexión con MinIO.
- La ejecución de un script de modelado registra runs en el proyecto de W&B correspondiente.

En caso de error, revisar especialmente:
- API key de W&B definida en el `.env`.
- Claves de acceso a MinIO.
- Conectividad de red o VPN si aplica.

---

## Despliegue con Podman (Contenedores)

El proyecto incluye un `Dockerfile` para empaquetar y desplegar la API del proyecto fácilmente mediante contenedores. Se recomienda el uso de [Podman](https://podman.io/) (aunque los comandos son idénticos si usas Docker).

### 1. Construir la imagen

Abre una terminal en la raíz del proyecto y ejecuta el siguiente comando para construir la imagen localmente. Esto instalará automáticamente todas las dependencias del proyecto utilizando `uv`:

```bash
podman build -t ignis-ai .
```

### 2. Ejecutar el contenedor

Una vez construida la imagen, levanta el contenedor con el siguiente comando:

```bash
podman run -d -p 8000:8000 --env-file .env --name ignis-app ignis-ai
```

**Explicación de los parámetros:**
- `-d`: Ejecuta el contenedor en segundo plano (*detached*).
- `-p 8000:8000`: Expone el puerto 8000 del contenedor (donde escucha la API) en el puerto 8000 de tu máquina local.
- `--env-file .env`: Carga automáticamente las credenciales y configuraciones importantes (como accesos a MinIO o W&B) desde tu archivo `.env`.
- `--name ignis-app`: Asigna el nombre `ignis-app` al contenedor para facilitar su administración en pasos posteriores.

Una vez en ejecución, la API estará accesible desde tu navegador o cliente REST en: `http://localhost:8000`.

### 3. Comandos útiles de gestión

Para ver los logs en tiempo real (útil para ver peticiones a la API o errores):
```bash
podman logs -f ignis-app
```

Para detener el contenedor:
```bash
podman stop ignis-app
```

Para iniciar un contenedor que habías detenido previamente:
```bash
podman start ignis-app
```

Para eliminar el contenedor (por ejemplo, si necesitas recrearlo tras un cambio en el código):
```bash
podman rm -f ignis-app
```

---

## Guía de Uso de la API

La API de IgnisAI permite integrar las capacidades de predicción de incendios en otras aplicaciones o scripts. Está construida con **FastAPI** y ofrece predicciones basadas en modelos XGBoost entrenados con datos meteorológicos y satelitales.

### Documentación Interactiva
Una vez que la API está en ejecución, puedes acceder a la documentación interactiva y probar los endpoints directamente desde el navegador:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### Funcionalidades Principales
1. **Predicción de Ocurrencia**: Estima la probabilidad de que ocurra un incendio en una ubicación y fecha específicas.
2. **Estimación de Intensidad (FRP)**: Predice la potencia radiativa del fuego (Fire Radiative Power) si ocurriera un incendio.
3. **Streaming de Progreso (SSE)**: Permite recibir actualizaciones en tiempo real mientras el sistema extrae las variables necesarias para la predicción.
4. **Consulta Histórica**: Acceso a datos históricos de incendios en formato GeoJSON.

### Parámetros de Solicitud (Request Body)
Todos los endpoints de predicción aceptan un objeto JSON con la siguiente estructura:

| Campo | Tipo | Descripción | Requerido |
|-------|------|-------------|-----------|
| `latitud` | `float` | Latitud del punto geográfico. | Sí |
| `longitud` | `float` | Longitud del punto geográfico. | Sí |
| `fecha` | `string` | Fecha en formato `YYYY-MM-DD`. | No (por defecto hoy) |

### Endpoints de Predicción

#### 1. Ocurrencia de Incendio
- **Endpoint**: `POST /predict/ocurrencia`
- **Ejemplo con curl**:
  ```bash
  curl -X 'POST' \
    'http://localhost:8000/predict/ocurrencia' \
    -H 'Content-Type: application/json' \
    -d '{
    "latitud": 40.4167,
    "longitud": -3.7033,
    "fecha": "2026-05-10"
  }'
  ```

#### 2. Intensidad (FRP)
- **Endpoint**: `POST /predict/intensidad`
- **Ejemplo con curl**:
  ```bash
  curl -X 'POST' \
    'http://localhost:8000/predict/intensidad' \
    -H 'Content-Type: application/json' \
    -d '{
    "latitud": 40.4167,
    "longitud": -3.7033
  }'
  ```

### Endpoints de Streaming (Server-Sent Events)
Estos endpoints son ideales para interfaces de usuario, ya que notifican cada paso del proceso (extracción de datos meteorológicos, topográficos, etc.) antes de devolver el resultado final.

- **Endpoints**: 
  - `POST /predict/ocurrencia/stream`
  - `POST /predict/intensidad/stream`

### Otros Endpoints

- **GeoJSON Histórico**: `GET /geojson/{year}`
  - Ejemplo: `GET /geojson/2024` devuelve los incendios detectados en ese año.
- **Imágenes de Satélite**: `GET /imagen/{filename}`
  - Recupera imágenes almacenadas en el servidor MinIO.

---
## Extraer el CSV de incendios

Para descargar los datos, sigue estos pasos:

1. Entra a [NASA FIRMS](https://firms.modaps.eosdis.nasa.gov/download/).
2. Pon tu correo y dale a **Email code**. Te llegará un código que debes introducir en la siguiente pantalla.
3. Pulsa en **Create New Request**.
4. Donde pone *World*, selecciona **Custom Region** y pon en el campo `-10,35,35,75` (este es el recuadro que cubre Europa).
5. En *Fire Source*, selecciona **VIIRS NOAA-20**.
6. Rellena las fechas deseadas (inicio y fin de los datos).
7. Pon el formato en **.csv**, mantén marcada la última caja y dale a **Submit**.

Suele tardar unos 10 minutos aproximadamente. Podrás descargar el archivo en la página siguiente.

## Configuración de Google Earth Engine

Solo es necesaria si se quiere re-ejecutar la extracción de datos desde cero. Para ello:

1. Ve a [Google Earth Engine](https://earthengine.google.com/), haz clic en **"Get Started"** y selecciona tu cuenta de Google.
2. Selecciona **"Consultar si cumples con los requisitos para el uso no comercial"**.
3. Rellena los datos de tu organización (tipo: Institución académica) y haz clic en **"Verificar requisitos"**.
4. Elige el plan **"Comunidad"** y registra tu proyecto.
5. Habilita la **API de Google Earth Engine** en tu proyecto de Google Cloud.
6. Ve a **"API y servicios" → "Credenciales"** y crea una cuenta de servicio con el rol **"Administrador de recursos de Earth Engine"**.
7. Genera una clave en formato JSON, renómbrala a `google-credentials.json` y añade su ruta al `.env` como `RUTA_CREDENCIALES`.

## Mapa Interactivo Mapbox 3D

### Requisitos Previos

- [Node.js](https://nodejs.org/) instalado.
- Una cuenta de [Mapbox](https://account.mapbox.com/) para obtener un token de acceso (API Key).
- Activar la VPN de la Complutense para poder cargar fotos y mapas desde el servidor MinIO

### Configuración Inicial

1. **Instalar dependencias**:
   Abre una terminal en la carpeta de mapa-ignis y ejecuta:
   ```bash
   npm install
   ```

2. **Configurar el Token de Mapbox**:
   Como medida de seguridad, el token de Mapbox se carga desde una variable de entorno. 
   
   - Abre el archivo `.env` y reemplaza el valor con tu propio token de Mapbox:
     ```env
     VITE_MAPBOX_TOKEN=tu_token_de_mapbox_aqui
     ```

### Ejecución del Mapa

Para iniciar el servidor de desarrollo, ejecuta (en la carpeta de mapa-ignis) :

```bash
npm run dev
```

Luego, abre la URL que aparece en la terminal (usualmente `http://localhost:5173/`) en tu navegador.

---

## Equipo

Proyecto desarrollado en el marco de la asignatura *Proyecto de Datos I* — Universidad Complutense de Madrid, 2025–2026.

---

## Licencia

Proyecto académico — uso interno UCM. Todos los derechos reservados.
