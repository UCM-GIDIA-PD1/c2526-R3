import sys
from pathlib import Path
import os

ruta_script = Path(__file__).resolve()  
ruta_src = ruta_script.parent.parent.parent 
ruta_extraccion = ruta_src / "extraccion"

sys.path.append(str(ruta_extraccion))

try:
    import minioFunctions
    print("minioFunctions cargado")
except ImportError as e:
    sys.exit(1)

import wandb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
from wandb.sklearn import plot_roc, plot_precision_recall, plot_feature_importances
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

wandb.require("core") 
os.environ["WANDB_START_METHOD"] = "thread"

minioFunctions.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')

cliente = minioFunctions.crear_cliente()
df = minioFunctions.cargar_dataset_incendios(eliminar_correladas = False)

X = df.drop(["final", "date"], axis=1)
y = df["final"]
class_names = [str(c) for c in sorted(y.unique())]
feature_names = X.columns

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

def train():
    with wandb.init(settings=wandb.Settings(start_method="thread")) as run:
        config_dict = dict(wandb.config)
        
        umbral = config_dict.pop('umbral_decision', 0.5)
        
        params_xgb = {
            'random_state': 42,
            'use_label_encoder': False,
            'eval_metric': 'logloss'
        }
        params_xgb.update(config_dict)
        
        clf = xgb.XGBClassifier(**params_xgb)

        clf.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )
        
        y_probas = clf.predict_proba(X_test)
        y_pred = (y_probas[:, 1] >= umbral).astype(int) 
        
        y_test_array = y_test.to_numpy()

        accuracy = (y_pred == y_test_array).mean()
        f1 = f1_score(y_test_array, y_pred)

        wandb.log({
            "accuracy": accuracy,
            "f1_score": f1,
            "roc_nativa": wandb.plot.roc_curve(y_test_array, y_probas, labels=class_names),
            "pr_nativa": wandb.plot.pr_curve(y_test_array, y_probas, labels=class_names),
            "matriz_confusion": wandb.plot.confusion_matrix(
                preds=y_pred, 
                y_true=y_test_array, 
                class_names=class_names
            )
        })

        plot_roc(y_test, y_probas, labels=class_names)
        plot_precision_recall(y_test, y_probas, labels=class_names)
        plot_feature_importances(clf)
        
        wandb.sklearn.plot_classifier(
            clf, X_train, X_test, y_train, y_test, 
            y_pred, y_probas, labels=class_names, 
            model_name="XGBoost", feature_names=feature_names
        )

        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close() 

if __name__ == "__main__":
    wandb.login(key=os.getenv('WANDB_KEY'))
    proyecto = "XGboost"
    entidad = "pd1-c2526-team3"

    configuraciones = {
        "1_bayesiano_conservador": {
            'method': 'bayes',
            'metric': {'name': 'f1_score', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'distribution': 'uniform', 'min': 0.05, 'max': 0.15},
                'max_depth': {'values': [4, 5, 6, 7]},
                'n_estimators': {'values': [200, 300, 400]},
                'scale_pos_weight': {'distribution': 'uniform', 'min': 1.0, 'max': 3.0},
                'umbral_decision': {'distribution': 'uniform', 'min': 0.35, 'max': 0.55}
            }
        },
        "2_random_exploratorio_pesos": {
            'method': 'random',
            'metric': {'name': 'f1_score', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'distribution': 'uniform', 'min': 0.01, 'max': 0.2},
                'max_depth': {'values': [3, 5, 8]},
                'n_estimators': {'values': [100, 500]},
                'scale_pos_weight': {'values': [2, 4, 6]},
                'umbral_decision': {'values': [0.4, 0.45, 0.5, 0.6]}
            }
        },
        "3_bayesiano_penalizado": {
            'method': 'bayes',
            'metric': {'name': 'f1_score', 'goal': 'maximize'},
            'parameters': {
                'learning_rate': {'values': [0.1]},
                'max_depth': {'values': [6, 8, 10]},
                'n_estimators': {'values': [300]},
                'reg_alpha': {'values': [0.1, 1, 5]},
                'reg_lambda': {'values': [1, 10]},
                'scale_pos_weight': {'values': [2, 3]},
                'umbral_decision': {'distribution': 'uniform', 'min': 0.4, 'max': 0.5}
            }
        }
    }

    runs_por_configuracion = 15

    for nombre_config, config in configuraciones.items():
        print(f"\nIniciando Sweep: {nombre_config}")
        sweep_id = wandb.sweep(config, project=proyecto, entity=entidad)
        wandb.agent(sweep_id, function=train, count=runs_por_configuracion)