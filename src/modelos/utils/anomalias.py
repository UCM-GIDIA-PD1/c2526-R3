from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import LocalOutlierFactor
from limpieza import transformacion
from modelos.utils import particiones as part
from sklearn.decomposition import PCA

SEED = 42

def isolationForest(X_train, X_val, X_test):
    '''
    Entrena un IsolationForest utilizado para detectar anomalías y les pone
    su score de que tan raros son en la columna 'anomaly'.
    '''

    
    bosque = IsolationForest(random_state = SEED,
                              n_estimators = 200,
                              n_jobs = -1)
    bosque.fit(X_train)

    train_anom = bosque.decision_function(X_train)
    test_anom = bosque.decision_function(X_test)
    val_anom = bosque.decision_function(X_val)

    X_train['anomaly_ISOL_FOR'] = train_anom
    X_val['anomaly_ISOL_FOR'] = val_anom
    X_test['anomaly_ISOL_FOR'] = test_anom

    return X_train, X_val, X_test

def oneClassSVM(X_train_PCA, X_val_PCA, X_test_PCA, X_train, X_val, X_test):
    '''
    Divide el espacio en el que están los datos dividiendolos por planos.
    Es necesario escalar los datos ya que este método funciona por distancias,
    también es necesario eliminar las variables muy correladas entre sí ya que
    a este modelo le afectan mucho.
    '''

    svm = OneClassSVM(gamma='auto', cache_size=1000)
    svm.fit(X_train_PCA)

    train_anom = svm.decision_function(X_train_PCA)
    test_anom = svm.decision_function(X_test_PCA)

    val_anom = svm.decision_function(X_val_PCA)

    X_train['anomaly_SVM'] = train_anom
    X_val['anomaly_SVM'] = val_anom
    X_test['anomaly_SVM'] = test_anom

    return X_train, X_val, X_test

def LOF(X_train_PCA, X_val_PCA, X_test_PCA, X_train, X_val, X_test):
    '''
    Local Outlier Factor, detecta anomalías basándose en la densidad de los datos.
    Es necesario escalar los datos ya que este método funciona por distancias,
    también es necesario eliminar las variables muy correladas entre sí ya que
    a este modelo le afectan mucho.
    '''

    
    lof = LocalOutlierFactor(contamination=0.05, n_neighbors=25, n_jobs = -1, novelty= True)

    lof.fit(X_train_PCA)

    train_anom = lof.decision_function(X_train_PCA)
    test_anom = lof.decision_function(X_test_PCA)
    val_anom = lof.decision_function(X_val_PCA)

    X_train['anomaly_LOF'] = train_anom
    X_val['anomaly_LOF'] = val_anom
    X_test['anomaly_LOF'] = test_anom

    return X_train, X_val, X_test

def escalado_PCA(X_train, X_val, X_test):
    '''
    Realiza el escalado con un StandardScaler y posteriormente un PCA,
    devolviendo los datasets transformados.
    '''

    scaler = StandardScaler()
    scaler.fit(X_train)

    X_train_esc = scaler.transform(X_train)
    X_val_esc = scaler.transform(X_val)
    X_test_esc = scaler.transform(X_test)

    pca = PCA(n_components=0.97, random_state=SEED)

    X_train_PCA = pca.fit_transform(X_train_esc)
    X_val_PCA = pca.transform(X_val_esc)
    X_test_PCA = pca.transform(X_test_esc)


    return X_train_PCA, X_val_PCA, X_test_PCA