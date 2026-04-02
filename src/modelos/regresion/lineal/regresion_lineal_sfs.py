import numpy as np
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from collections import Counter

import wandb
import modelos.utils.carga_datos as cg
from modelos.utils.particiones import split_temporal, generador_cv
import modelos.utils.wandbFunctions as wf
import modelos.utils.personalizacion as pers

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "regLinealMultiple"
SEED = 42
NUM_IT = 0

def evaluacion_final(hiperparametros, metodo):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    
    config_final = {
        "method": metodo,
        "parameters": {
            "n_features_to_select": {"values": [hiperparametros["n_features_to_select"]]},
            "direction": {"values": [hiperparametros["direction"]]},
            "tol": {"values": [hiperparametros["tol"]]}
        }
    }
        
    sweep_id_final = wandb.sweep(config_final, entity=WANDB_ENTITY, project=WANDB_PROJECT)

    def agente_final():
        evaluacion(X_train_full, X_test, y_train_full, y_test, metodo)

    wandb.agent(sweep_id_final, function=agente_final, count=1)


def evaluacion(X_train_full, X_test, y_train_full, y_test, metodo):
    run = wandb.init(tags=["Evaluacion Final", "LinearRegression_SFS", metodo]) 
    config = wandb.config

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_full)
    X_test_scaled = scaler.transform(X_test)

    modelo_base = LinearRegression()
    sfs = SequentialFeatureSelector(
        modelo_base,
        n_features_to_select=config.n_features_to_select,
        direction=config.direction,
        tol=config.tol,
        scoring="r2",
        cv=5,
        n_jobs=-1
    )

    sfs.fit(X_train_scaled, y_train_full)
    features_finales = X_train_full.columns[sfs.get_support()].tolist()
    
    X_train_sfs = sfs.transform(X_train_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)

    X_train_sm = sm.add_constant(X_train_sfs)
    modelo_sm = sm.OLS(y_train_full.values, X_train_sm).fit()
    
    X_test_sm = sm.add_constant(X_test_sfs)
    y_pred_test = modelo_sm.predict(X_test_sm)
    residuos = modelo_sm.resid

    rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
    mae_test = mean_absolute_error(y_test, y_pred_test)
    r2_test = r2_score(y_test, y_pred_test)

   
    _, p_shapiro = stats.shapiro(residuos) if len(residuos) < 5000 else (0, 0)
    test_bp = sms.het_breuschpagan(residuos, X_train_sm)
    p_bp = test_bp[1]


    wandb.log({
        "test/rmse": float(rmse_test),
        "test/mae": float(mae_test),
        "test/r2": float(r2_test),
        "p_valor_shapiro": p_shapiro,
        "p_valor_breusch_pagan": p_bp,
        "n_features_final": len(features_finales),
        "features_seleccionadas_list": features_finales
    })


    coef_data = [[f, c] for f, c in zip(features_finales, modelo_sm.params[1:])]
    table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
    wandb.log({"feature_importance_final": wandb.plot.bar(table, "Feature", "Coefficient", title="Pesos Variables Elegidas (Final)")})

    run.finish()


def entrenamiento(X_train_full, y_train_full, metrica_elegida, nombre=None):
    global NUM_IT
    NUM_IT += 1

    run = wf.wandb_init(WANDB_PROJECT, nombre, NUM_IT)
    config = wandb.config

    cv_generator = generador_cv(tipo_cv="temporal", n_splits=4, seed=SEED)
    

    rmse_cv_val, mae_cv_val, r2_cv_val = [], [], []
    rmse_cv_train, mae_cv_train, r2_cv_train = [], [], []
    all_selected_features = []
    n_features_list = []

    for train_idx, val_idx in cv_generator.split(X_train_full, y_train_full):
        X_fold_train = X_train_full.iloc[train_idx]
        y_fold_train = y_train_full.iloc[train_idx]
        X_fold_val = X_train_full.iloc[val_idx]
        y_fold_val = y_train_full.iloc[val_idx]

        sc = StandardScaler()
        X_f_train_sc = sc.fit_transform(X_fold_train)
        X_f_val_sc = sc.transform(X_fold_val)

        modelo_fold = LinearRegression()
        sfs_fold = SequentialFeatureSelector(
            modelo_fold,
            n_features_to_select=config.n_features_to_select,
            direction=config.direction,
            tol=config.tol,
            scoring="r2",
            cv=3
        )
        
        sfs_fold.fit(X_f_train_sc, y_fold_train)
        
        fold_features = X_train_full.columns[sfs_fold.get_support()].tolist()
        all_selected_features.extend(fold_features)
        n_features_list.append(len(fold_features))

        X_f_train_sfs = sfs_fold.transform(X_f_train_sc)
        X_f_val_sfs = sfs_fold.transform(X_f_val_sc)

        modelo_fold.fit(X_f_train_sfs, y_fold_train)
        
        y_train_pred = modelo_fold.predict(X_f_train_sfs)
        y_val_pred = modelo_fold.predict(X_f_val_sfs)

        rmse_cv_train.append(np.sqrt(mean_squared_error(y_fold_train, y_train_pred)))
        mae_cv_train.append(mean_absolute_error(y_fold_train, y_train_pred))
        r2_cv_train.append(r2_score(y_fold_train, y_train_pred))

        rmse_cv_val.append(np.sqrt(mean_squared_error(y_fold_val, y_val_pred)))
        mae_cv_val.append(mean_absolute_error(y_fold_val, y_val_pred))
        r2_cv_val.append(r2_score(y_fold_val, y_val_pred))

    feature_freq = Counter(all_selected_features)
    freq_data = [[f, count] for f, count in feature_freq.items()]
    freq_table = wandb.Table(data=freq_data, columns=["Feature", "Frequency_in_CV"])
  
    wandb.log({
        "train/rmse_mean_cv": float(np.mean(rmse_cv_train)),
        "train/mae_mean_cv": float(np.mean(mae_cv_train)),
        "train/r2_mean_cv": float(np.mean(r2_cv_train)),
        "val/rmse_mean_cv": float(np.mean(rmse_cv_val)),
        "val/r2_mean_cv": float(np.mean(r2_cv_val)),
        "val/mae_mean_cv": float(np.mean(mae_cv_val)),
        "cv/avg_n_features": float(np.mean(n_features_list)),
        "cv/feature_selection_stability": wandb.plot.bar(freq_table, "Feature", "Frequency_in_CV", title="Frecuencia de Selección en CV"),
        "overfitting_gap_rmse": float(np.mean(rmse_cv_val) - np.mean(rmse_cv_train))
    })

    run.finish()


def inicializar():
    if not wf.inicializar_apikey_wandb():
        return
    X, y = pers.pregunta_PCA(clasificacion=False)
    X = X.drop(columns = ['log_frp'])
    X_train_full, X_test, y_train_full, y_test = split_temporal(X, y, date_col='date', test_size=0.2)
    X_train_full, X_test = pers.anomalias(X_train_full, X_test)
    return X_train_full, X_test, y_train_full, y_test


def regresion(metodo_elegido, metrica_elegida):
    X_train_full, X_test, y_train_full, y_test = inicializar()
    iters, nombre = pers.pregunta_iters_nombre()

    def ent():
        entrenamiento(X_train_full, y_train_full, metrica_elegida, nombre)

    params = {
        "n_features_to_select": ["auto", 5, 8, 12, 15] if metodo_elegido == "grid" else {"values": ["auto", 5, 10, 15]},
        "direction": ["forward", "backward"] if metodo_elegido == "grid" else {"values": ["forward", "backward"]},
        "tol": [0.001, 0.01] if metodo_elegido == "grid" else {"distribution": "log_uniform_values", "min": 0.0001, "max": 0.01}
    }

    metrica_limpia = metrica_elegida.lower().strip()
    sweep_config = {
        "name": f"RegLinealMultiple-SFS-{metodo_elegido}-{metrica_limpia}",
        "method": metodo_elegido, 
        "metric": {"name": f"val/{metrica_limpia}_mean_cv", "goal": "maximize" if metrica_limpia == "r2" else "minimize"},
        "parameters": params
    }

    sweep_id = wandb.sweep(sweep_config, entity=WANDB_ENTITY, project=WANDB_PROJECT)
    wandb.agent(sweep_id=sweep_id, function=ent, count=iters)

if __name__ == "__main__":
    metodo = input("\n Selecciona el método (grid, random o bayes): " )
    metrica = input("\n Selecciona la métrica (rmse/mae/r2): " )
    regresion(metodo, metrica)


#Conclusiones:
#Como ya veíamos con regresión lineal sin selección de variables, el coeficiente de determinación
#era de tan solo 0.063. 
#Además, las conclusiones de linealidad, homocedasticidad y normalidad no se cumplían.

#Con este nuevo, el R^2 ha subido ligeramente a 0.09 para train y alcanzando como mucho un valor de 0.008 para validación, lo que muestra la presencia de overfitting.

#Se han seleccionado, principalmente, area_ha, precipitation, temp_mean, pressure_mean