# c2526-R3

# IgnisAI
<img width="2581" height="1245" alt="Gemini_Generated_Image_wr92ufwr92ufwr92" src="https://github.com/user-attachments/assets/55b90c3c-eead-4330-b572-862d93f18a51" />



## Descripción
IgnisAI es un sistema diseñado para predecir la probabilidad de ignición y estimar el comportamiento de incendios forestales (intensidad, velocidad y superficie) basándose en variables meteorológicas y topológicas. El objetivo es proporcionar una herramienta de apoyo a la toma de decisiones que permita a diversos actores (administración pública, sector privado, gestores forestales...) anticiparse al riesgo y optimizar la asignación de recursos de emergencia.

## Estructura del repositorio
```text
IgnisAI/
├── src/
│   ├── extracción/
│   │   ├── construccion_df.py
│   │   ├── filtros_no_sinteticos.py
│   │   ├── fisicas.py
│   │   ├── incendios.py
│   │   ├── mascaras.py
│   │   ├── minioFunctions.py
│   │   ├── parquet.py
│   │   ├── pendiente.py
│   │   ├── puntos_sinteticos.py
│   │   ├── vegetacion.py
│   │   └── vegetacion2.py
│   └── main.py
├── .gitignore
├── .python-version
├── README.md
├── pyproject.toml
└── uv.lock
```

## Estructura de los datos en MinIO
```text
pd1/
└── grupo3/
    └── raw/
        ├── Biogeoregiones/
        ├── Countries/
        ├── Fisicas/
        ├── incendios/ # nomenclatura incendios_{año}.parquet
        ├── Incendios_y_no_incendios/ # nomenclatura incendios_y_no_incendios_{año}.parquet
        ├── Pendiente/
        ├── No_incendios/ # nomenclatura no_incendios_{año}.parquet
        ├── Vegetacion/   
        └── Vegetacion2/
```

Para las características de Fisicas, Pendiente, Vegetación y Vegetación2 la nomenclatura de archivos dentro de sus respectivas carpetas es incendios_y_no_incendios_{característica}_{año}.parquet

## Configuración de Google Earth Engine
Para poder utilizar este proyecto, es necesario crear un proyecto en Google Cloud, registrarse en Earth Engine y generar unas credenciales de acceso, siguiendo los pasos:

1. Ve a [Google Earth Engine](https://earthengine.google.com/), haz clic en **"Get Started"** y selecciona la cuenta de Google con la que deseas crear el proyecto.
2. En la pantalla de Configuración, de las dos opciones que aparecen, selecciona **"Consultar si cumples con los requisitos para el uso no comercial"**.
3. Rellena los datos de tu organización:
   - **Tipo de organización:** Institución académica.
   - **Institución:** (Tu universidad).
   - **Alcance geográfico:** Regional -> Europa.
   - Haz clic en **"Verificar requisitos"**.
4. Elige el plan **"Comunidad"** y haz clic en Continuar.
5. Haz clic en **"Registrar"**.

6. Una vez en tu proyecto, **habilita la API de Google Earth Engine**.
7. Haz clic en el icono de las tres barras arriba a la izquierda, pon el cursor en el apartado **"API y servicios"** y selecciona **"Credenciales"**.
8. Baja hasta el apartado de *Cuentas de servicio* y pulsa en **"Administrar cuentas de servicio"**.
9. Haz clic en **"Crear cuenta de servicio"** y asígnale el nombre que prefieras.
10. En la sección del rol, busca y selecciona **"Administrador de recursos de Earth Engine"**. (El último paso opcional no es necesario rellenarlo). Haz clic en **"Listo"**.

11. Una vez creada la cuenta de servicio, haz clic en ella dentro de la lista para ver sus detalles y ve a la pestaña **"Claves"**.
12. Haz clic en **"Agregar clave"** → **"Crear una nueva"** (elige el formato JSON). Se te descargará un archivo `.json` automáticamente.
13. Cambia el nombre de ese archivo descargado a `google-credentials.json` (si vas a meterlo dentro de la carpeta del proyecto).

14. Por último, añade la ruta de este archivo a tus variables de entorno bajo el nombre de `RUTA_CREDENCIALES`.

## Configuración de MinIO
El proyecto cuenta con funciones de conexión con MinIO. Principalmente, se usan funciones para subir y bajar ficheros sin necesidad de trabajar en local. Para que esto funcione es necesario añadir a tus variables de entorno:
1. ACCESS_KEY de MinIO, con nombre AWS_ACCESS_KEY_ID.
2. SECRET_KEY de MinIO, con nombre AWS_SECRET_ACCESS_KEY. 
