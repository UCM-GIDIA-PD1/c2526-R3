import wandb
import yaml
from dotenv import load_dotenv
import os

WANDB_ENTITY = "pd1-c2526-team3"

def wandb_init(project, nombre = None, it = -1):
    '''
    Inicializa un run de wandb con el nombre y número de iteración especificados.
    Si no se especifica un nombre o no se están contando las iteraciones, se usará 
    el formato por defecto de wandb para evitar sobreescribir runs anteriores.
    '''
    if nombre is None or it == -1:
        return wandb.init(
            entity=WANDB_ENTITY,
            project = project,
        )
    
    return wandb.init(
        entity=WANDB_ENTITY,
        project=project,
        name = f'{nombre}-{it}'
    )

def cargar_configuracion(ruta_yaml):
    """Carga la configuración del sweep desde el YAML."""
    with open(ruta_yaml, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def crear_sweep_id(project, ruta_yaml):
    """
    Crea un sweep id a partir de un YAML
    """
    config_sweep = cargar_configuracion(ruta_yaml)
    sweep_id = wandb.sweep(config_sweep, entity=WANDB_ENTITY, project=project)

    return sweep_id

def inicializar_apikey_wandb():
    '''
    Carga la API key de wandb desde las variables de entorno y la establece para su uso.
    Devuelve True si la clave se cargó correctamente, o False si no se encontró.
    '''
    load_dotenv()
    api_key = os.getenv("WANDB_KEY")

    if not api_key:
        print("API key de wandb no encontrada. Acuérdate de ponerla como WANDB_KEY en el archivo .env")
        return False
    
    os.environ["WANDB_API_KEY"] = api_key
    return True

def matriz_confusion_feature_importance(model, y_pred, y, features):
    '''
    Muestra la matriz de confusión y la importancia de las features en wandb.
    '''
    wandb.sklearn.plot_confusion_matrix(y, y_pred, labels=["no_incendio", "incendio"])
        
    wandb.sklearn.plot_feature_importances(model, feature_names= features)
