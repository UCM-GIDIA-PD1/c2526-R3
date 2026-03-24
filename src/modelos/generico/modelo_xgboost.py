#sys.path.append(str(Path(__file__).resolve().parents[2] / "extraccion"))
from extraccion import minioFunctions 
import wandb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score, recall_score, precision_score, roc_auc_score
from wandb.sklearn import plot_roc, plot_precision_recall, plot_feature_importances
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from modelos import parser
from modelos.utils import personalizacion as per, wandbFunctions as wf
from modelos.clasificacion import ventanas_temporales as vt

def menu():
    print("Opciones: ")
    print("1. XGBoost con configuraciones normales")
    print("2. XGBoost con aplicación de ventanas temporales y anomalías")
    
    opcion = int(input("Elige opcion [1,2]: "))
    assert opcion == 1 or opcion == 2, "Número no válido"

    return opcion

def funcionalidad_tags():
    #Inicializamos parser para pasar las características de nuestro modelo por consola
    args = parser.initialite_parser()

    tags = []
    if args.modelo: 
        tags.append(args.modelo)
    tags = args.tags + [f"correladas_{args.eliminar_correladas}"]

    return tags

def configuraciones_iniciales():
    tags = funcionalidad_tags()

    #Conexión con MinIO
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, "grupo3/cleaned/final_date_transformado.parquet", "df")

    #Creación de nuestras variables explicativas y respuesta
    X = df.drop(["final"], axis=1)
    y = df["final"]
    class_names = ["No Incendio", "Incendio"]
    feature_names = X.columns

    #División de los datos en entrenamiento y validación
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    return tags, class_names, feature_names, X_train, X_test, y_train, y_test

def ventanas_temporales_y_anomalias():
    #Tags
    tags = funcionalidad_tags()

    #MinIO
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, "grupo3/cleaned/archivo_definitivo.parquet", "df")

    #Primer paso: aplicación de ventanas temporales
    df = vt.crear_features_temporales(df)

    #Segundo paso: aplicación de PCA
    X, y = per.pregunta_PCA()

    #Tercer paso: división temporal
    X_train, X_val, X_test, y_train, y_val, y_test = vt.split_temporal(X, y)

    #Cuarto paso: anomalías
    X_train, X_val, X_test = per.anomalias(X_train, X_val, X_test)

    #Nombres columnas
    num_columnas = X_train.shape[1]
    feature_names = [f"Variable_{i}" for i in range(num_columnas)]
    class_names = ["No Incendio", "Incendio"]

    return tags, class_names, feature_names, X_train, X_test, y_train, y_test

def train(tags, class_names, feature_names, X_train, X_test, y_train, y_test):
    with wandb.init(settings=wandb.Settings(start_method="thread"),
                    tags=tags) as run:
        
        config_dict = dict(wandb.config)
        umbral = config_dict.pop('umbral_decision', 0.5)
        
        params_xgb = {
            'random_state': 42,
            'use_label_encoder': False,
            'eval_metric': 'logloss'
        }
        params_xgb.update(config_dict)
        clf = xgb.XGBClassifier(**params_xgb)

        clf.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
        
        y_probas = clf.predict_proba(X_test)
        y_pred = (y_probas[:, 1] >= umbral).astype(int) 
        y_test_array = y_test.to_numpy()

        # Métricas
        accuracy = (y_pred == y_test_array).mean()
        f1 = f1_score(y_test_array, y_pred)
        recall = recall_score(y_test_array, y_pred)
        precision = precision_score(y_test_array, y_pred)
        
        # Mostramos en WANDB
        wandb.log({
            "metricas/accuracy": accuracy,
            "metricas/f1_score": f1,
            "metricas/recall": recall,
            "metricas/precision": precision,
            "graficas/roc_nativa": wandb.plot.roc_curve(y_test_array, y_probas, labels=class_names),
            "graficas/pr_nativa": wandb.plot.pr_curve(y_test_array, y_probas, labels=class_names)
        })

        plot_roc(y_test, y_probas, labels=class_names)
        plot_precision_recall(y_test, y_probas, labels=class_names)
        
        wf.matriz_confusion_feature_importance(clf, y_pred, y_test_array, feature_names)

        wandb.sklearn.plot_classifier(
            clf, X_train, X_test, y_train, y_test, 
            y_pred, y_probas, labels=class_names, 
            model_name="XGBoost", feature_names=feature_names
        )

        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close()

if __name__ == "__main__":
    assert wf.inicializar_apikey_wandb(), "Error: No se pudo cargar la API Key de WandB."
    wandb.login() 

    proyecto = "XGboost"
    entidad = "pd1-c2526-team3"

    configuraciones = {
        "random_ventanas_anomalias": {
            'method': 'random',
            'metric': {'name': 'f1_score', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'distribution': 'uniform', 'min': 0.01, 'max': 0.2},
                'max_depth': {'values': [3, 5, 8]},
                'n_estimators': {'values': [100, 300]},
                'scale_pos_weight': {'values': [3, 4, 5]},
                'umbral_decision': {'distribution': 'uniform', 'min': 0.4, 'max': 0.5}
            }
        },

        "bayesiano_ventanas_anomalias": {
            'method': 'bayes',
            'metric': {'name': 'f1_score', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'distribution': 'uniform', 'min': 0.05, 'max': 0.15},
                'max_depth': {'values': [6, 7, 8]},
                'n_estimators': {'values': [200, 250, 300]},
                'reg_alpha': {'values': [0.1, 1, 5]},
                'reg_lambda': {'values': [1, 10]},
                'scale_pos_weight': {'distribution': 'int_uniform', 'min': 5, 'max': 20},
                'umbral_decision': {'distribution': 'uniform', 'min': 0.25, 'max': 0.5},
                'subsample': {'distribution': 'uniform', 'min': 0.7, 'max': 0.9},
                'colsample_bytree': {'distribution': 'uniform', 'min': 0.7, 'max': 0.9},
                'min_child_weight': {'values': [1, 3, 5]}
            }
        },
    }

    runs_por_configuracion = 15
    opcion = menu()
    
    if opcion == 1:
        tags, class_names, feature_names, X_train, X_test, y_train, y_test = configuraciones_iniciales()
    else:
        tags, class_names, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales_y_anomalias()
    
    for nombre_config, config in configuraciones.items():
        print(f"\nIniciando Sweep: {nombre_config}")
        config['name'] = nombre_config         
        sweep_id = wandb.sweep(config, project=proyecto, entity=entidad)
        wandb.agent(sweep_id, function=lambda: train(tags, class_names, feature_names, X_train, X_test, y_train, y_test), count=runs_por_configuracion)