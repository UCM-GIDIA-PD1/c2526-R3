from minio import Minio
from pathlib import Path

'''
Comprobar:
- Archivo keys.txt en el directorio del proyecto con las claves del server.
- Tener VPN de la Complu activada.
'''

def importar_keys():
    path = Path(__file__).resolve().parent.parent.parent / "keys.txt"
    assert path.exists(), "Archivo con las claves keys.txt no encontrado."

    with open(path, "r") as f:
        keys = f.readlines()
        acces_key = keys[0].strip()
        secret_key = keys[1].strip()
    
    return acces_key, secret_key

def crear_cliente():
    ak, sk = importar_keys()
    
    cliente = Minio(
        endpoint="minio.fdi.ucm.es",
        access_key = ak,
        secret_key = sk
    )

    return cliente
    
def subir_fichero(cliente, path_local: Path, path_server: Path):
    assert path_local.exists(), "ruta no existente"
    source_file = path_local
    destination_file = path_server
    
    cliente.fput_object(
        bucket_name=path_server,
        object_name=destination_file,
        file_path=source_file,
    )
    print("Fichero subido :)")
    
def bajar_fichero(cliente, path_server: Path, path_local: Path):
    source_file = path_server
    destination_file = path_local
    
    cliente.fget_object(
        bucket_name="pd1",
        object_name=source_file,
        file_path=destination_file,
    )
    print("Fichero cargado :)")

cliente = crear_cliente()
path = Path(__file__).resolve().parent.parent.parent / "data/prueba.txt"
bajar_fichero(cliente, "comun/pruebaG3.txt", path)