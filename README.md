# IgnisAI

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Status](https://img.shields.io/badge/status-en%20desarrollo-yellow)
![UCM](https://img.shields.io/badge/UCM-Proyecto%20de%20Datos%20I-red)

<img width="2581" height="1245" alt="Gemini_Generated_Image_wr92ufwr92ufwr92" src="https://github.com/user-attachments/assets/55b90c3c-eead-4330-b572-862d93f18a51" />

## Descripción

IgnisAI es un sistema diseñado para predecir la probabilidad de ignición y estimar el comportamiento de incendios forestales (intensidad, velocidad y superficie) basándose en variables meteorológicas y topológicas. El objetivo es proporcionar una herramienta de apoyo a la toma de decisiones que permita a diversos actores (administración pública, sector privado, gestores forestales...) anticiparse al riesgo y optimizar la asignación de recursos de emergencia.

---

## Requisitos previos

Antes de comenzar, asegúrate de tener lo siguiente:

- Python >= 3.12 (probado con 3.12.8)
- [`uv`](https://github.com/astral-sh/uv) instalado como gestor de dependencias
- Cuenta activa en [Google Earth Engine](https://earthengine.google.com/)
- Acceso al servidor MinIO del proyecto

---

## Estructura del repositorio

```text
c2526-R3/
    ├── src/
    │   ├── extraccion/
    │   │   ├── descartadas/
    │   │   │   ├── vegetacion2.py
    │   │   │   └── suelo.py
    │   │   ├── construccion_df.py
    │   │   ├── filtros_no_sinteticos.py
    │   │   ├── fisicas.py
    │   │   ├── incendios.py
    │   │   ├── mascaras.py
    │   │   ├── minioFunctions.py
    │   │   ├── parquet.py
    │   │   ├── pendiente.py
    │   │   ├── puntos_sinteticos.py
    │   │   └── vegetacion.py
    │   ├── limpieza.ipynb
    │   ├── analisis.ipynb
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
    │   ├── Biogeoregiones/ ...        #.shp
    │   ├── mapa/ ...                  #.tif
    │   ├── Incendios_firms/ ...       #.csv
    │   └── SOC/ ...                   #.tif
    ├── raw/
    │   ├── Biogeoregiones/ ...
    │   ├── Final/ ...
    │   ├── Fisicas/ ...
    │   ├── Incendios_y_no_incendios/ ...
    │   ├── incendios/ ...
    │   ├── No_incendios/ ...
    │   ├── Pendiente/ ...
    │   ├── Soil_organic_carbon/ ...
    │   ├── Vegetacion/ ...
    │   └── Vegetacion2/ ...
    ├── cleaned/
    |   ├── modeloGeneral.parquet
    |   └── modeloIncendios.parquet
    ├── processed/
    └── grupo.txt
```

Para las características de Fisicas, Pendiente, Vegetación y Vegetación2, la nomenclatura de archivos dentro de sus respectivas carpetas es `incendios_y_no_incendios_{característica}_{año}.parquet`.

---

## Configuración de Google Earth Engine

Para poder utilizar este proyecto, es necesario crear un proyecto en Google Cloud, registrarse en Earth Engine y generar unas credenciales de acceso, siguiendo los pasos:

1. Ve a [Google Earth Engine](https://earthengine.google.com/), haz clic en **"Get Started"** y selecciona la cuenta de Google con la que deseas crear el proyecto.
2. En la pantalla de Configuración, selecciona **"Consultar si cumples con los requisitos para el uso no comercial"**.
3. Rellena los datos de tu organización:
   - **Tipo de organización:** Institución académica.
   - **Institución:** (Tu universidad).
   - **Alcance geográfico:** Regional -> Europa.
   - Haz clic en **"Verificar requisitos"**.
4. Elige el plan **"Comunidad"** y haz clic en Continuar.
5. Haz clic en **"Registrar"**.
6. Una vez en tu proyecto, **habilita la API de Google Earth Engine**.
7. Haz clic en el icono de las tres barras arriba a la izquierda, pon el cursor en **"API y servicios"** y selecciona **"Credenciales"**.
8. Baja hasta *Cuentas de servicio* y pulsa en **"Administrar cuentas de servicio"**.
9. Haz clic en **"Crear cuenta de servicio"** y asígnale el nombre que prefieras.
10. En la sección del rol, busca y selecciona **"Administrador de recursos de Earth Engine"**. Haz clic en **"Listo"**.
11. Una vez creada la cuenta de servicio, haz clic en ella y ve a la pestaña **"Claves"**.
12. Haz clic en **"Agregar clave"** → **"Crear una nueva"** (formato JSON). Se descargará un archivo `.json` automáticamente.
13. Renombra ese archivo a `google-credentials.json`.
14. Añade la ruta de este archivo a tus variables de entorno bajo el nombre `RUTA_CREDENCIALES`.

---

## Configuración de MinIO

El proyecto cuenta con funciones de conexión con MinIO para subir y bajar ficheros sin necesidad de trabajar en local. Para que esto funcione, añade a tus variables de entorno:

1. `AWS_ACCESS_KEY_ID` → ACCESS KEY de MinIO.
2. `AWS_SECRET_ACCESS_KEY` → SECRET KEY de MinIO.

---

## Configuración del archivo `.env`

Crea un archivo `.env` en la raíz del proyecto (`c2526-R3/`) con la siguiente estructura:

```env
RUTA_CREDENCIALES=/ruta/completa/a/google-credentials.json
AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
```

### Descripción de las variables

| Variable | Descripción |
|----------|-------------|
| `RUTA_CREDENCIALES` | Ruta absoluta al archivo `google-credentials.json` descargado desde Google Cloud. |
| `AWS_ACCESS_KEY_ID` | Clave de acceso para la conexión con MinIO. |
| `AWS_SECRET_ACCESS_KEY` | Clave secreta asociada a la cuenta de MinIO. |

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

### 3. Ejecutar el sistema

```bash
uv run python src/main.py
```

El archivo `main.py` actúa como orquestador de los distintos módulos de extracción y procesamiento.

### 4. Verificación de correcto funcionamiento

El sistema se considera correctamente configurado si:

- No aparecen errores de autenticación con Google Earth Engine.
- No aparecen errores de conexión con MinIO.
- La ejecución de `main.py` finaliza sin excepciones.
- Se generan archivos `.parquet` en las rutas esperadas.

En caso de error, revisar especialmente:
- Ruta definida en `RUTA_CREDENCIALES`.
- Claves de acceso a MinIO.
- Conectividad de red o VPN si aplica.

---

## Equipo

Proyecto desarrollado en el marco de la asignatura *Proyecto de Datos I* — Universidad Complutense de Madrid, 2025.

---

## Licencia

Proyecto académico — uso interno UCM. Todos los derechos reservados.

