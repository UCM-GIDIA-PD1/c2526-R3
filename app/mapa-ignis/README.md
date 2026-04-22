# Mapa Interactivo Mapbox 3D

Este es un proyecto básico configurado con Vite y Mapbox GL JS para renderizar un mapa interactivo con relieve en 3D.

## Requisitos Previos

- [Node.js](https://nodejs.org/) instalado.
- Una cuenta de [Mapbox](https://account.mapbox.com/) para obtener un token de acceso (API Key).

## Configuración Inicial

1. **Instalar dependencias**:
   Abre una terminal en esta carpeta y ejecuta:
   ```bash
   npm install
   ```

2. **Configurar el Token de Mapbox**:
   Como medida de seguridad, el token de Mapbox se debe cargar desde una variable de entorno. 
   
   - Copia el archivo `.env.example` y renómbralo a `.env` (si no existe ya un archivo `.env`).
   - Abre el archivo `.env` y reemplaza el valor con tu propio token de Mapbox:
     ```env
     VITE_MAPBOX_TOKEN=tu_token_de_mapbox_aqui
     ```

## Ejecución del Proyecto

Para iniciar el servidor de desarrollo, ejecuta:

```bash
npm run dev
```

Luego, abre la URL que aparece en la terminal (usualmente `http://localhost:5173/`) en tu navegador de preferencia.
