import wandb
import pandas as pd
import os
import matplotlib.pyplot as plt
import statsmodels.api as sm
import statsmodels.stats.api as sms
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from wandb.sklearn import plot_residuals
import extraccion.minioFunctions as mf

# Configuración de API Key
mf.load_dotenv()
os.environ['WANDB_API_KEY'] = os.getenv('WANDB_KEY')

# 1. Extraer datos
l_f = []
for k in [2022, 2023, 2024, 2025]:
    print(f"Analizando el año {k}:")
    dataf = mf.bajar_fichero(mf.crear_cliente(), path_server=f"grupo3/cleaned/Modelo_Incendios_{k}.parquet", type="df")
    l_f.append(dataf)

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

# 3. Entrenamiento (Sklearn para predicción rápida)
modelo = LinearRegression()
modelo.fit(X_train_scaled, y_train) 
y_pred = modelo.predict(X_test_scaled)

# 4. Ajuste adicional con Statsmodels (para tests de hipótesis y gráficos técnicos)
X_train_sm = sm.add_constant(X_train_scaled)
modelo_sm = sm.OLS(y_train, X_train_sm).fit()
residuos = modelo_sm.resid

# 5. Inicializar W&B
run = wandb.init(
    entity="pd1-c2526-team3", 
    project='regLinealMultiple', 
    config={
        "model": "LinearRegression",
        "features": list(X.columns),
        "test_size": 0.2
    }
)

# 6. TESTS DE HIPÓTESIS (Resultados del Notebook)
# Shapiro-Wilk (Normalidad)
stat_shapiro, p_shapiro = stats.shapiro(residuos)
# Breusch-Pagan (Homocedasticidad)
test_bp = sms.het_breuschpagan(residuos, X_train_sm)
p_bp = test_bp[1]

# Subir p-valores al summary
wandb.run.summary["p_valor_shapiro"] = p_shapiro
wandb.run.summary["p_valor_breusch_pagan"] = p_bp

# 7. GRÁFICOS

# QQ-Plot
# fig_qq = plt.figure(figsize=(6, 6))
# sm.qqplot(residuos, line='45', ax=fig_qq.gca())
# plt.title("QQ-Plot de Residuos")
# wandb.log({"qq_plot": wandb.Image(fig_qq)})
# plt.close(fig_qq)

# Gráfico de Leverage
# fig_lev = plt.figure(figsize=(8, 6))
# sm.graphics.influence_plot(modelo_sm, ax=fig_lev.gca())
# plt.title("Gráfico de Leverage")
# wandb.log({"leverage_plot": wandb.Image(fig_lev)})
# plt.close(fig_lev)

# 8. MÉTRICAS ESTÁNDAR
r2_test = r2_score(y_test, y_pred)
r2_train = modelo_sm.rsquared 

wandb.log({
    "r2_test": r2_test,
    "r2_train": r2_train,
    "mse": mean_squared_error(y_test, y_pred),
    "mae": mean_absolute_error(y_test, y_pred)
})

wandb.run.summary["final_r2_test"] = r2_test

# 9. Importancia de Variables
coef_data = [[f, c] for f, c in zip(X.columns, modelo.coef_)]
table = wandb.Table(data=coef_data, columns=["Feature", "Coefficient"])
wandb.log({"feature_importance": wandb.plot.bar(table, "Feature", "Coefficient", title="Pesos del Modelo")})

run.finish()