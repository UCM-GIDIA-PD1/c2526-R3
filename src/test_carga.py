from modelos.utils.carga_datos import cargar_dataset_general

from modelos.utils.particiones import split_estratificado

X, y = cargar_dataset_general()
print(X.columns.tolist())

X_train, X_val, X_test, y_train, y_val, y_test = split_estratificado(X, y)