from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import wandb
import xgboost as xgb
import yaml
from sklearn.metrics import f1_score, recall_score, fbeta_score
from extraccion import minioFunctions 
from modelos import parser
from modelos.utils import personalizacion as per, wandbFunctions as wf, explicabilidad as exp
from modelos.utils.particiones import split_temporal, generador_cv, oversampling
from modelos.utils.metricas import evaluar_clasificacion
from modelos.clasificacion import ventanas_temporales as vt

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "XGboost"
SEED = 42

def menu():
    print("Opciones: ")
    print("1. XGBoost con aplicación de ventanas temporales")
    print("2. XGBoost con aplicación de ventanas temporales y anomalías")
    opcion = int(input("Elige opcion [1,2]: "))
    assert opcion in [1, 2], "Número no válido."
    print("¿Qué dataframe quieres usar?")
    df = input("Nombre del dataframe en MinIO (p.e. 'MINI'): ")
    if not df:
        df = "MINI"
    return opcion, df

def funcionalidad_tags():
    args = parser.initialite_parser()
    tags = []
    if args.modelo: 
        tags.append(args.modelo)
    tags = args.tags + [f"correladas_{args.eliminar_correladas}"]
    return tags

def configuraciones_iniciales(df):
    tags = funcionalidad_tags()
    cliente = minioFunctions.crear_cliente()
    df = minioFunctions.bajar_fichero(cliente, f"grupo3/cleaned/{df}.parquet", "df")
    assert df is not None
    if 'final' in df.columns:
        df = df.rename(columns={'final': 'incendio'})
    assert 'incendio' in df.columns
    return tags, df

def ventanas_temporales(df):
    tags, df_normal = configuraciones_iniciales(df)
    df_transformado = vt.menu_ventanas_temporales(df_normal)
    X, y = per.pregunta_PCA(df_transformado) 
    X_train, X_test, y_train, y_test = split_temporal(X, y)
    feature_names = [f"Var_{i}" for i in range(X_train.shape[1])]
    return tags, feature_names, X_train, X_test, y_train, y_test

def ventanas_temporales_y_anomalias(df):
    tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales(df)
    X_train, X_test = per.anomalias(X_train, X_test)
    feature_names = [f"Var_{i}" for i in range(X_train.shape[1])]
    return tags, feature_names, X_train, X_test, y_train, y_test

def explicabilidad_lime(clasificador, X_train, X_test):
    X_train_lime = X_train.fillna(0)
    X_test_lime = X_test.fillna(0)
    explicador = exp.inicializar_explicador(X_train_lime)
    explicacion_lime = exp.generar_explicacion(explicador, clasificador, X_test_lime)
    fig_lime = explicacion_lime.as_pyplot_figure()
    plt.tight_layout()
    wandb.log({"explicabilidad/lime": wandb.Image(fig_lime)})
    plt.close(fig_lime)

def train(tags, class_names, feature_names, X_train_full, X_test, y_train_full, y_test):
    X_train_full = X_train_full.fillna(0)
    X_test = X_test.fillna(0)
    
    with wandb.init(settings=wandb.Settings(start_method="thread"), tags=tags) as run:
        config = wandb.config
        clf = xgb.XGBClassifier(
            n_estimators=getattr(config, 'n_estimators', 100),
            learning_rate=getattr(config, 'learning_rate', 0.1),
            max_depth=getattr(config, 'max_depth', 6),
            scale_pos_weight=getattr(config, 'scale_pos_weight', 1), 
            subsample=getattr(config, 'subsample', 0.8),
            colsample_bytree=getattr(config, 'colsample_bytree', 0.8),
            gamma=getattr(config, 'gamma', 0),
            min_child_weight=getattr(config, 'min_child_weight', 1),
            random_state=SEED,
            eval_metric='logloss',
            n_jobs=-1 
        )

        cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
        cv_f1, cv_f2, cv_recall, mejor_umbrales_cv = [], [], [], []
        
        for t_idx, v_idx in cv_generator.split(X_train_full, y_train_full):
            x_f_t, x_f_v = X_train_full.iloc[t_idx], X_train_full.iloc[v_idx]
            y_f_t, y_f_v = y_train_full.iloc[t_idx], y_train_full.iloc[v_idx]
            
            x_f_t_res, y_f_t_res = oversampling(x_f_t, y_f_t, proporcion_incendios=0.33)
            
            clf.fit(x_f_t_res, y_f_t_res)
            y_f_prob = clf.predict_proba(x_f_v)[:, 1]
            
            u_opt = vt.encontrar_mejor_umbral(y_f_v, y_f_prob)
            mejor_umbrales_cv.append(u_opt)
            
            y_f_pred = (y_f_prob >= u_opt).astype(int)
            cv_f1.append(f1_score(y_f_v, y_f_pred, zero_division=0))
            cv_f2.append(fbeta_score(y_f_v, y_f_pred, beta=2, zero_division=0))
            cv_recall.append(recall_score(y_f_v, y_f_pred, zero_division=0))

        X_train_res, y_train_res = oversampling(X_train_full, y_train_full, proporcion_incendios=0.33)
        clf.fit(X_train_res, y_train_res)
        
        u_final = np.mean(mejor_umbrales_cv) if mejor_umbrales_cv else 0.5
        y_probas = clf.predict_proba(X_test)
        y_pred = (y_probas[:, 1] >= u_final).astype(int)
        
        metricas = evaluar_clasificacion(y_test, y_pred, y_probas[:, 1], "Test")
        
        wandb.log({
            "val/f1_mean_cv": np.mean(cv_f1),
            "val/f2_mean_cv": np.mean(cv_f2),
            "val/recall_mean_cv": np.mean(cv_recall),
            "val/umbral_medio_cv": u_final,
            "test/f1": metricas["f1"],
            "test/recall": metricas["recall"],
            "test/precision": metricas["precision"],
            "test/accuracy": metricas["accuracy"],
            "graficas/roc": wandb.plot.roc_curve(y_test, y_probas, labels=class_names),
            "graficas/pr": wandb.plot.pr_curve(y_test, y_probas, labels=class_names)
        })
        
        wf.matriz_confusion_feature_importance(clf, y_pred, y_test.to_numpy(), feature_names)
        xgb.plot_importance(clf)
        wandb.log({"importancia_variables_xgb": wandb.Image(plt)})
        plt.close()
        explicabilidad_lime(clf, X_train_full, X_test)

    
def entrenar():
    assert wf.inicializar_apikey_wandb()
    wandb.login() 
    opcion, df = menu()
    if opcion == 1:
        tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales(df)
    elif opcion == 2:
        tags, feature_names, X_train, X_test, y_train, y_test = ventanas_temporales_y_anomalias(df)
    
    class_names = ["No Incendio", "Incendio"]

    with open("sweep_config.yaml", "r") as f:
        sweep_config = yaml.safe_load(f)

    sweep_id = wandb.sweep(sweep_config, project=WANDB_PROJECT, entity=WANDB_ENTITY)
    wandb.agent(sweep_id, function=lambda: train(tags, class_names, feature_names, X_train, X_test, y_train, y_test), count=15)

if __name__ == "__main__":
    entrenar()
    