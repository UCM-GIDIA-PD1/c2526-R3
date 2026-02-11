import requests
import pandas as pd
from io import StringIO
import os 
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("INTENSIDAD_API_KEY")

def fuerza_incendios(api_key): #utiliza nasa FIRMS
  url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/VIIRS_SNPP_NRT/world/5" #creo que no puedo poner aqui directamente mi api key
  response = requests.get(url)
  if(response.status_code != 200):
    print("Error en la solicitud")
  else:
    df = pd.read_csv(StringIO(response.text))
    df = df[df['confidence'] != 'l'] #confianza l (low) es que no es fiable
    df['acq_date'] = pd.to_datetime(df['acq_date'])
    df = df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date']) #quita incendios iguales
    df['delta_t'] = df['bright_ti4'] - df['bright_ti5'] #si es mayor de 25K es de alta intensidad y si es mayor de 60 es poco fiable
    columnas_utiles = ['latitude', 'longitude','delta_t', 'frp', 'acq_date']
    return df[columnas_utiles]
