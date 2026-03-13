import os
import sys
import numpy as np
import pandas as pd # Necesario para la concatenación correcta

import wandb
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.feature_selection import SequentialFeatureSelector
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))
from modelos.utils.carga_datos import cargar_dataset_incendios
from modelos.utils.particiones import split_regresion
from modelos.utils.metricas import evaluar_regresion

os.environ["WANDB_API_KEY"] = os.getenv("WANDB_KEY", "")

WANDB_ENTITY = "pd1-c2526-team3"
WANDB_PROJECT = "regLinealMultiple"

def main():
    # 1. Cargar dataset
    X, y_log = cargar_dataset_incendios(eliminar_correladas=False, logs=False)

    # 2. Split inicial
    X_train, X_val, X_test, y_train, y_val, y_test = split_regresion(X, y_log)

    #SELECCIÓN Y VALIDACIÓN
    scaler_init = StandardScaler()
    X_train_scaled = scaler_init.fit_transform(X_train)
    X_val_scaled = scaler_init.transform(X_val)

    # Selección de variables (solo con train)
    modelo_base = LinearRegression()
    sfs = SequentialFeatureSelector(
        modelo_base,
        n_features_to_select="auto",
        tol=0.001,
        direction="forward",
        scoring="r2",
        cv=5,
        n_jobs=-1,
    )

    print("Ejecutando SFS...")
    sfs.fit(X_train_scaled, y_train)
    features_seleccionadas = X.columns[sfs.get_support()].tolist()

    # Reducción a variables seleccionadas para validación
    X_train_sfs = sfs.transform(X_train_scaled)
    X_val_sfs = sfs.transform(X_val_scaled)

    # Evaluación inicial en validación
    modelo_temp = LinearRegression()
    modelo_temp.fit(X_train_sfs, y_train)
    y_pred_val = modelo_temp.predict(X_val_sfs)

    # REENTRENAMIENTO FINAL (Train + Val)
    X_trainval = pd.concat([X_train, X_val])
    y_trainval = pd.concat([y_train, y_val])

    # ESCALADO NUEVO
    scaler_final = StandardScaler()
    X_trainval_scaled = scaler_final.fit_transform(X_trainval)
    X_test_scaled = scaler_final.transform(X_test)


    X_trainval_sfs = sfs.transform(X_trainval_scaled)
    X_test_sfs = sfs.transform(X_test_scaled)

    modelo_final = LinearRegression()
    modelo_final.fit(X_trainval_sfs, y_trainval)

    # Predicción final en TEST
    y_pred_test = modelo_final.predict(X_test_sfs)

    X_trainval_sm = sm.add_constant(X_trainval_sfs)
    modelo_sm = sm.OLS(y_trainval.values, X_trainval_sm).fit()
    residuos = modelo_sm.resid

    
    run = wandb.init(
        entity=WANDB_ENTITY,
        project=WANDB_PROJECT,
        config={
            "model": "LinearRegression_SFS_Refit",
            "features_seleccionadas": features_seleccionadas,
            "n_features_final": len(features_seleccionadas),
            "split": "regresion_80_10_10",
        },
    )

    # Tests estadísticos
    _, p_shapiro = stats.shapiro(residuos) if len(residuos) < 5000 else (0, 0)
    test_bp = sms.het_breuschpagan(residuos, X_trainval_sm)
    p_bp = test_bp[1]

    wandb.run.summary["p_valor_shapiro"] = p_shapiro
    wandb.run.summary["p_valor_breusch_pagan"] = p_bp
    wandb.run.summary["r2_trainval"] = modelo_sm.rsquared

    # Métricas
    metricas_val = evaluar_regresion(y_val, y_pred_val, "Validación — Regresión lineal SFS", en_log=False)
    metricas_test = evaluar_regresion(y_test, y_pred_test, "Test — Regresión lineal SFS (Refit)", en_log=False)

    wandb.log({
        **{f"val/{k}": v for k, v in metricas_val.items()},
        **{f"test/{k}": v for k, v in metricas_test.items()}
    })

    # Importancia de variables
    coef_data = [[f, c] for f, c in zip(features_seleccionadas, modelo_final.coef_)]
    table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
    wandb.log({"feature_importance": wandb.plot.bar(table, "Feature", "Coefficient", title="Pesos Modelo Final (SFS)")})

    run.finish()

if __name__ == "__main__":
    main()

#Conclusiones:
#Como ya veíamos con regresión lineal sin selección de variables, el coeficiente de determinación
#era de tan solo 0.023 para los datos de validación y 0.029 para test. 
#Además, las conclusiones de linealidad, homocedasticidad y normalidad no se cumplían.

#Con este nuevo, el R^2 ha subido ligeramente a 0.0277 para validación y ha bajado a 0.0084 para test, y las variables seleccionadas han sido NDVI, evapotranspiration, grados,
#area_ha y wind_gusts_max.

#Esto coincide exactamente con el análisis que hicimos de correlaciones. Se ha seleccionado únicamente uno de los índices 
#de vegetación, dado que ambos representaban lo mismo. También ha seleccionado tan solo una de las variables
#que expresan la pendiente, puesto que las tres que tenemos en el conjunto de datos están altamente correlacionadas,
#las hectáreas quemadas y una de las variables que expresan el viento. 

#Con este nuevo modelo con menos variables siguen sin cumplirse las hipótesis de linealidad y normalidad,
#lo que supone la necesidad de descartar el modelo, aunque ahora cumpla la hipótesis de homocedasticidad. 