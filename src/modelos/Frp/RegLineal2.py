import wandb
import pandas as pd
import os
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from sklearn.feature_selection import SequentialFeatureSelector
import extraccion.minioFunctions as mf

# Configuración de API Key
mf.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')

# 1. Extraer datos
l_f = []
for k in [2022, 2023, 2024, 2025]:
    print(f"Analizando el año {k}:")
    try:
        dataf = mf.bajar_fichero(mf.crear_cliente(), path_server=f"grupo3/cleaned/Modelo_Incendios_{k}.parquet", type="df")
        l_f.append(dataf)
    except Exception as e:
        print(f"Error cargando año {k}: {e}")

df_total = pd.concat(l_f, ignore_index=True)

datos = df_total.drop(columns=['date_last', 'count', 'lat', 'lon', 'frp_sum', 'final', 'duration_days', 'date'])

X = datos.drop(columns=['frp_mean'])
y = datos['frp_mean']

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 2. Escalado
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train) 
X_test_scaled = scaler.transform(X_test)

# 3. FORWARD SELECTION (Equivalente al forwarding de R)
modelo_base = LinearRegression()

#tol representa cuánto mínimo debe aportar una variable para que sea incluida en el modelo
#n_jobs sirve para indicar el nivel de paralelismo en el ordenador. -1 hace que el proceso sea lo más rápido posible
sfs = SequentialFeatureSelector(
    modelo_base, 
    n_features_to_select="auto", 
    tol=0.001, 
    direction='forward', 
    scoring='r2', 
    cv=5, 
    n_jobs=-1
)

sfs.fit(X_train_scaled, y_train)

# Filtrar variables seleccionadas
X_train_sfs = sfs.transform(X_train_scaled)
X_test_sfs = sfs.transform(X_test_scaled)
features_seleccionadas = X.columns[sfs.get_support()].tolist()

# 4. Entrenamiento 
modelo_final = LinearRegression()
modelo_final.fit(X_train_sfs, y_train) 
y_pred = modelo_final.predict(X_test_sfs)

# 5. Ajuste con Statsmodels para p-valores
X_train_sm = sm.add_constant(X_train_sfs)
modelo_sm = sm.OLS(y_train, X_train_sm).fit()
residuos = modelo_sm.resid

# 6. Inicializar W&B
run = wandb.init(
    entity="pd1-c2526-team3", 
    project='regLinealMultiple', 
    config={
        "model": "LinearRegression_SFS",
        "features_originales": list(X.columns),
        "features_seleccionadas": features_seleccionadas,
        "n_features_final": len(features_seleccionadas),
        "test_size": 0.2
    }
)

# 7. TESTS DE HIPÓTESIS
stat_shapiro, p_shapiro = stats.shapiro(residuos) if len(residuos) < 5000 else (0, 0) # Shapiro falla en datasets muy grandes
test_bp = sms.het_breuschpagan(residuos, X_train_sm)
p_bp = test_bp[1]

wandb.run.summary["p_valor_shapiro"] = p_shapiro
wandb.run.summary["p_valor_breusch_pagan"] = p_bp

# 8. MÉTRICAS
r2_test = r2_score(y_test, y_pred)
r2_train = modelo_sm.rsquared 

wandb.log({
    "r2_test": r2_test,
    "r2_train": r2_train,
    "mse": mean_squared_error(y_test, y_pred),
    "mae": mean_absolute_error(y_test, y_pred)
})

# 9. Importancia de Variables (Solo las seleccionadas)
coef_data = [[f, c] for f, c in zip(features_seleccionadas, modelo_final.coef_)]
table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
wandb.log({"feature_importance": wandb.plot.bar(table, "Feature", "Coefficient", title="Pesos del Modelo Seleccionado")})

run.finish()


#Conclusiones:
#Como ya veíamos con regresión lineal sin selección de variables, el coeficiente de determinación
#era de tan solo 0.024 para los datos de validación. 
#Además, las conclusiones de linealidad, homocedasticidad y normalidad no se cumplían.

#Con este nuevo, el R^2 ha bajado a 0.021, y las variables seleccionadas han sido NDVI, temp_max, porcentaje,
#area_ha y wind_gusts_max.

#Esto coincide exactamente con el análisis que hicimos de correlaciones. Ha eliminado las variables
#de temperatura, quedándose solo con una de ellas, se ha seleccionado únicamente uno de los índices 
#de vegetación, dado que ambos representaban lo mismo. También ha seleccionado tan solo una de las variables
#que expresan la pendiente, puesto que las tres que tenemos en el conjunto de datos están altamente correlacionadas,
#las hectáreas quemadas y una de las variables que expresan el viento. 

#Con este nuevo modelo con menos variables siguen sin cumplirse las hipótesis de linealidad y normalidad,
#lo que supone la necesidad de descartar el modelo, aunque ahora cumpla la hipótesis de homocedasticidad,
#con un valor de 0.037 aproximadamente. 