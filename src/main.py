from extraccion import construccion_df

def main():
    df_final = construccion_df.build_environmental_df('C:/Users/adiez/Desktop/2_CARRERA/Segundo_Cuatri/PD1/Prediccion incendios/fire_archive_J1V-C2_716870.csv')
    print(df_final.head())


if __name__ == "__main__":
    main()
