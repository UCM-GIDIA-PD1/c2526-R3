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
        ├── incendios/      # nomenclatura incendios_{año}.parquet
        ├── No_incendios/   
        └── Vegetacion/
```