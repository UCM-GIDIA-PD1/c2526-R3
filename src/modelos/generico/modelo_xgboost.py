import sys
from pathlib import Path
import os

# --- 1. CONFIGURACIÓN DE RUTAS (Llegar a src/extraccion) ---
ruta_script = Path(__file__).resolve()  
ruta_src = ruta_script.parent.parent.parent 
ruta_extraccion = ruta_src / "extraccion"

sys.path.append(str(ruta_extraccion))

# --- 2. IMPORTS LOCALES ---
try:
    import minioFunctions
    print("✅ minioFunctions cargado correctamente desde src/extraccion")
except ImportError as e:
    print(f"❌ Error: No se pudo encontrar minioFunctions en {ruta_extraccion}")
    sys.exit(1)

# --- 3. IMPORTS DE LIBRERÍAS ---
import wandb
import xgboost as xgb
from sklearn.model_selection import train_test_split
from wandb.sklearn import (
    plot_roc, plot_precision_recall, plot_feature_importances
)
import matplotlib
matplotlib.use('Agg') # Fuerza a Matplotlib a generar imágenes sin abrir ventanas GUI
import matplotlib.pyplot as plt
# --- CONFIGURACIÓN CRÍTICA PARA ESTABILIDAD ---
wandb.require("core") 
os.environ["WANDB_START_METHOD"] = "thread"

# --- 4. PREPARACIÓN DE DATOS ---
minioFunctions.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')

cliente = minioFunctions.crear_cliente()
df = minioFunctions.bajar_fichero(cliente, "grupo3/cleaned/final.parquet", "df")

X = df.drop(["final", "date"], axis=1)
y = df["final"]
class_names = [str(c) for c in sorted(y.unique())]
feature_names = X.columns

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# --- 5. FUNCIÓN DE ENTRENAMIENTO ---
def train():
    with wandb.init(settings=wandb.Settings(start_method="thread")) as run:
        config = wandb.config
        
        clf = xgb.XGBClassifier(
            n_estimators=config.n_estimators,
            learning_rate=config.learning_rate,
            max_depth=config.max_depth,
            subsample=config.subsample,
            random_state=42,
            use_label_encoder=False 
        )

        clf.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )

        y_pred = clf.predict(X_test)
        y_probas = clf.predict_proba(X_test)
        accuracy = (y_pred == y_test).mean()

        # --- SOLUCIÓN AL ERROR ---
        # Convertimos la Serie de Pandas a un Array de Numpy para que W&B no colapse
        y_test_array = y_test.to_numpy()

        # Log de métricas y gráficas interactivas nativas
        wandb.log({
            "accuracy": accuracy,
            "roc_nativa": wandb.plot.roc_curve(y_test_array, y_probas, labels=class_names),
            "pr_nativa": wandb.plot.pr_curve(y_test_array, y_probas, labels=class_names),
            "matriz_confusion": wandb.plot.confusion_matrix(probs=y_probas, y_true=y_test_array, class_names=class_names)
        })

        # Gráficas de Scikit-Learn
        plot_roc(y_test, y_probas, labels=class_names)
        plot_precision_recall(y_test, y_probas, labels=class_names)
        plot_feature_importances(clf)
        
        wandb.sklearn.plot_classifier(
            clf, X_train, X_test, y_train, y_test, 
            y_pred, y_probas, labels=class_names, 
            model_name="XGBoost", feature_names=feature_names
        )

        # Gráfica de importancia de XGBoost
        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close() 

# --- 6. BLOQUE PRINCIPAL ---
if __name__ == "__main__":
    wandb.login(key=os.getenv('WANDB_KEY'))

    sweep_config = {
        'method': 'random', 
        'metric': {'name': 'accuracy', 'goal': 'maximize'},
        'parameters': {
            'learning_rate': {'values': [0.01, 0.1, 0.3]},
            'max_depth': {'values': [3, 6, 9]},
            'n_estimators': {'values': [100, 200, 500]},
            'subsample': {'distribution': 'uniform', 'min': 0.5, 'max': 1.0}
        }
    }

    sweep_id = wandb.sweep(sweep_config, project="XGboost", entity="pd1-c2526-team3")
    print("\n🚀 Iniciando el Sweep de W&B...")
    wandb.agent(sweep_id, function=train, count=10)