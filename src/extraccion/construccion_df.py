import incendios
import pendiente
import vegetacion
import fisicas
import time
import pandas as pd
import vegetacion2

def build_environmental_df(path, limit=20,sleep=0.2):
    df_original, fires = incendios.fetch_fires(path)
    env_rows = []
    ini = time.time()
    src = vegetacion2.abrirMapa()
    data = src.read(1)
    vegetacion_df = pd.read_csv("src/mapa_vegetacion.csv", delimiter = ",")
    for _, row in fires.iterrows():
        env = fisicas.fetch_environment(row.lat_mean, row.lon_mean, row.date_first)
        pend = pendiente.pendiente(row.lat_mean, row.lon_mean)
        veg = vegetacion.vegetacion(row.lat_mean, row.lon_mean)
        veg2 = vegetacion2.entorno(row.lat_mean, row.lon_mean, src, data, vegetacion_df)
        env.update(pend)
        env.update(veg)
        env.update(veg2)
        env_rows.append(env)
        fin = time.time()
        print(fin - ini)
        time.sleep(sleep)

    vegetacion2.cerrarMapa()
    env_df = pd.DataFrame(env_rows)

    final_df = pd.concat(
        [fires.reset_index(drop=True), env_df],
        axis=1
    )

    return final_df