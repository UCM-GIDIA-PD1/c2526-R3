# 1. Python 3.12 
FROM python:3.12-slim

# 2. Carpeta de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar los archivos
COPY pyproject.toml README.md ./

# 4. Instalar las dependencias 
RUN pip install .

# 5. Copiar el resto del código
COPY . .

# 6. Exponer el puerto 
EXPOSE 8000

# 7. Comando para iniciar tu servidor
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]