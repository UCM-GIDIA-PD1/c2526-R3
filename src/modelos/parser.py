import argparse

def initialite_parser():
    '''
    Desde consola podremos añadir características a nuestros modelos entrenados para que queden más 
    organizados en Weight & Bias. Ejemplo:
    uv run modelo_xgboost.py --modelo xgboost --tags todas_variables --eliminar_correladas

    :return args: podemos acceder a los elementos de cada "argumento" con tag.tags (lista) o tags.eliminar_correladas (booleano)

    '''
    parser = argparse.ArgumentParser(description="características de los modelos para riesgo de incendios")

    #Tipo de modelo
    parser.add_argument('--modelo')

    #Tipo de split
    parser.add_argument(
        "--split",
        choices=["simple", "estratificado", "pesos", "todos"],
        default="todos"
    )
    
    #Características de nuestro modelo: todas_variables, eliminadas_variables, pca,... 
    parser.add_argument('--tags', 
                        nargs='+', 
                        default=["xgboost"], 
    )

    #Generar gráficas que queremos guardar (por reg logística)
    parser.add_argument("--no-graficas", 
                        action="store_true")

    #--eliminar_correladas cuando se ha utilizado la táctica de eliminación de variables correladas 
    parser.add_argument('--eliminar_correladas', 
                        action='store_true', 
    )

    args = parser.parse_args()

    return args
