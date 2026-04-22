import requests
import json

url = "http://localhost:8000/predict/ocurrencia"
data = {
    "latitud": 40.4168,
    "longitud": -3.7038,
    "fecha": "2026-04-22"
}

try:
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
except Exception as e:
    print(f"Error: {e}")
