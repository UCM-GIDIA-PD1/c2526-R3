
def limpieza(df):
    df = df.drop(['date_last'], axis = 1)

    df1 = df.copy()
    df2 = df.copy()
    df3 = df.copy()
    df4 = df.copy()

    df1 = df1.drop(['temp_min', 'temp_max', 'sunshine_seconds', 'evapotranspiration', 'NDWI', 'elevacion_centro', 'porcentaje', 'wind_gusts_max'], axis = 1)
    df2 = df2.drop(['temp_min', 'temp_mean', 'sunshine_seconds', 'radiation', 'NDVI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)
    df3 = df3.drop(['temp_min', 'temp_mean', 'sunshine_seconds', 'evapotranspiration', 'NDVI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)
    df4 = df4.drop(['temp_min', 'temp_max', 'sunshine_seconds', 'evapotranspiration', 'NDWI', 'pressure_mean',  'porcentaje', 'wind_gusts_max'], axis = 1)

    return [df1.dropna(), df2.dropna(), df3.dropna(), df4.dropna()]