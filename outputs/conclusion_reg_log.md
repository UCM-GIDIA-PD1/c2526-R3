### Paso 1 — Exploración inicial del dataset

Antes de tocar ningún modelo lo primero fue entender qué datos tenemos. Ejecutamos `exploracion_inicial.py` que descargó los 4 años de datos de MinIO y nos dio esto:

**71.444 observaciones con 22 columnas.** De esas, 68.760 son no incendios y 2.684 son incendios. Eso es un ratio de **25.6x**, es decir, hay 25 veces más no incendios que incendios. Esto es el problema del desbalanceo de clases y es lo que va a condicionar todas las decisiones que tomemos.

También vimos que el FRP (la variable que queremos predecir en regresión) tiene una distribución muy sesgada hacia la derecha — la mayoría de incendios tienen FRP bajo pero hay algunos con valores muy altos. El skewness era 3.39, que es muy alto.

Aunque algo similar ya lo habíamos hecho en la anterior fase, necesitabamos esos datos para tener un poco de contexto reciente de los datos con los que contábamos.

---

### Paso 2 — Eliminación de variables por multicolinealidad

Como comentamos antes, la exploración y la fase anterior también nos dio la **matriz de correlaciones**. Encontramos varios pares de variables con correlación altísima, por encima de 0.95:

- `porcentaje` y `grados` tenían r=0.996. Son prácticamente la misma variable — una en grados y otra en porcentaje. Nos quedamos con `grados` y eliminamos `porcentaje`.
- `temp_max`, `temp_min` y `temp_mean` tenían correlaciones de 0.97-0.98 entre sí. Las tres miden temperatura del mismo día. Nos quedamos con `temp_mean` y eliminamos las otras dos.
- `pressure_mean` y `elevacion_centro` tenían r=0.985. La presión atmosférica es físicamente una función de la altitud, son la misma información. Eliminamos `pressure_mean`.

Esto lo pusimos en `carga_datos.py` para que todos los modelos lo hereden automáticamente. El resultado fue pasar de 22 columnas a **13 features limpias**.

---

### Paso 3 — Transformación del FRP (hecho pero no utilizado)

El FRP con skewness 3.39 viola la hipótesis de normalidad que necesita la regresión lineal múltiple. La solución es aplicar **log(1 + FRP)** antes de entrenar. Esto comprime los valores altos y hace la distribución mucho más simétrica y aproximadamente normal.

Cuando el modelo de regresión prediga, el resultado estará en escala logarítmica, así que habrá que aplicar la operación inversa `exp(predicción) - 1` para volver a megavatios reales. Esto también está en `carga_datos.py`.

---

### Paso 4 — Las tres estrategias de partición

Implementamos tres estrategias distintas en `particiones.py` precisamente para comparar cuál funciona mejor:

**Split simple:** divide aleatoriamente 80/10/10 sin ningún criterio. El problema es que con 3.8% de incendios, por puro azar algún split puede acabar con muy pocos incendios o incluso ninguno. Es el caso base que funciona mal pero hay que tenerlo para comparar.

**Split estratificado:** divide 80/10/10 pero garantizando que los tres conjuntos tengan la misma proporción de incendios. Si el total tiene 3.8% de incendios, train tiene 3.8%, validación tiene 3.8% y test tiene 3.8%. Vimos en los resultados que esto se cumplió exactamente: 2148, 268 y 268 incendios respectivamente.

**Split con pesos:** el split es igual que el estratificado, pero además le decimos al modelo que cada incendio vale 25 veces más que un no incendio durante el entrenamiento. El modelo deja de poder "hacer trampas" prediciendo siempre no incendio porque ahora equivocarse en un incendio le penaliza mucho más.

---

### Paso 5 — El modelo de regresión logística

Con los datos listos y las particiones definidas, entrenamos la regresión logística. Este modelo calcula la probabilidad de incendio como una función sigmoide de las variables. Si esa probabilidad supera un umbral (por defecto 0.5), predice incendio.

Antes de entrenar aplicamos **StandardScaler** que transforma todas las variables para que tengan media 0 y desviación típica 1. Esto es importante por dos razones: primero, el optimizador converge mucho mejor cuando las variables están en la misma escala; segundo, los coeficientes resultantes son comparables entre sí y nos dicen cuáles variables influyen más.

El scaler se entrena **solo con los datos de train** y luego se aplica a validación y test. Si lo entrenásemos con todos los datos estaríamos usando información del futuro para escalar el pasado, lo que se llama data leakage y contamina los resultados.

---

### Paso 6 — Resultados e interpretación

Los tres experimentos dieron esto:

Con **split simple y estratificado sin pesos**, el modelo casi ignora los incendios. Recall de 0.06-0.09 significa que solo detecta 1 de cada 12 incendios reales. El modelo aprende que predecir siempre "no incendio" le da 96% de accuracy, así que no se molesta en aprender los incendios.

Con **pesos**, el recall sube a 0.82 — detecta 8 de cada 10 incendios. El precio es que la precision baja a 0.13, es decir, de cada 10 alarmas que da, 9 son falsas. Esto es el trade-off habitual con clases desbalanceadas.

El **ROC-AUC de 0.90** en todos los casos es revelador: el modelo internamente sí distingue bien entre incendios y no incendios, el problema es el umbral de 0.5 que es demasiado exigente. Ajustando ese umbral se podría mejorar el F1, pero eso es trabajo de optimización de semanas posteriores.

Los coeficientes tienen sentido físico real: NDVI alto y humedad alta reducen la probabilidad de incendio (vegetación sana y húmeda no arde), mientras que más horas de sol y más evapotranspiración la aumentan (condiciones de sequía y calor).

El F1 de 0.23 es bajo pero esperado para un modelo lineal con este nivel de desbalanceo. Los modelos no lineales como KNN, Random Forest y XGBoost deberían mejorarlo notablemente.