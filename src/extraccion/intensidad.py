import requests
import pandas as pd
from io import StringIO
import os 
from dotenv import load_dotenv

'''
Este módulo se encarga de extraer la información de intensidad de los incendios utilizando la API de NASA FIRMS.
La función fuerza_incendios hace una solicitud a la API, procesa los datos y devuelve un DataFrame con la latitud, longitud, delta_t, frp y 
fecha de adquisición de los incendios. 
Se filtran los incendios con baja confianza y se eliminan los duplicados para obtener una lista más precisa de incendios activos.
 
La variable delta_t se calcula como la diferencia entre las temperaturas de los canales 4 y 5 (que captan distintas longitudes de onda), 
lo que ayuda a identificar la intensidad de los incendios.
La variable frp (Fire Radiative Power) también se incluye para evaluar la energía liberada por los incendios, lo que es útil para determinar su gravedad. 
'''

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
