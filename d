[33mcommit a9556fcfcbeb5723e328ad85bccf8671897a9dfe[m[33m ([m[1;36mHEAD[m[33m -> [m[1;32mmain[m[33m, [m[1;31morigin/main[m[33m, [m[1;31morigin/HEAD[m[33m)[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Sat Feb 21 12:34:45 2026 +0100

    Funciones de subida particulares eliminadas

[33mcommit 99b16681f471d2e8731edd9b16237c7cce1f8f8a[m
Merge: b5970f5 17b1769
Author: JuananAlonso <juananal@ucm.es>
Date:   Sat Feb 21 12:24:44 2026 +0100

    Merge branch 'rama-Ignacio'

[33mcommit 17b176963ebd82680ee82bded24f7e48a2d20bc0[m[33m ([m[1;31morigin/rama-Ignacio[m[33m, [m[1;32mrama-Ignacio[m[33m)[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Sat Feb 21 12:14:50 2026 +0100

    Merge parquets testeado y funcional

[33mcommit b5970f5a58844b31f18c838161ae9e05b3cd1996[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Sat Feb 21 12:09:36 2026 +0100

    Preguntar al testear si quieres subir el parquet
    
    Facilita la subida a minio de parquets y nos da la libertad de hacer pruebas con las siguientes funciones

[33mcommit e555d8d75bc354cbebef2218fea4707e00f73a5f[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Sat Feb 21 11:53:20 2026 +0100

    Update main
    
    Un main mejorado donde se puede probar la función crear sintéticos

[33mcommit b5db5490b5d8d999006d821a7671213507714bae[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Sat Feb 21 11:52:53 2026 +0100

    Update puntos_sintéticos
    
    Se le añade una semilla para poder probar las funciones y se corrigen ciertas credenciales y llamadas

[33mcommit 1327a67e4a856154c19927e8c757e82d91e8652d[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Sat Feb 21 11:34:55 2026 +0100

    Añadida lat, lon, date a la extracción ind. de vegetación2

[33mcommit 2856ac573217d64e66348f6992685482568feb2f[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Sat Feb 21 07:11:26 2026 +0100

    Añadido merge de parquets y estandarizado de columnas en los dataframes de las variables

[33mcommit 86240162bcd135d9bd9c5645bfaf07108887ef8b[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 17:38:13 2026 +0100

    Update puntos_sinteticos.py

[33mcommit c8543987b1824863c181f0b67f958f10192dea6d[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 16:55:12 2026 +0100

    Nuevos cambios
    
    Seguimos realizando modificaciones para conexión con MinIO

[33mcommit 895400d7bb430794ac15368e97de6158650e467b[m
Merge: f9bade7 57c3aba
Author: fperiott-eng <fperiott@ucm.es>
Date:   Fri Feb 20 16:27:11 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit f9bade74e9689f2bf94bf6f8356bc705bb637c24[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Fri Feb 20 16:26:59 2026 +0100

    Update puntos_sinteticos.py
    
    Extracción automatizada de los parquets de mapa para poder filtrar correctamente

[33mcommit 57c3aba2cda9a75ffa63e02b320e17635c312252[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 16:22:52 2026 +0100

    Correccion error

[33mcommit fa365c22dde522fe946a78ecc539e272978f6083[m
Merge: 6b6512b 6ad5452
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 16:18:35 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit 6b6512b0542b49870ceecfc40477b0249f756997[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 16:17:42 2026 +0100

    Automatizado para MinIO
    
    Lo modificamos para que lea directamente de MinIO

[33mcommit 6ad54522cdc8ca83bccc3521a028616894604a3c[m
Merge: efc464c df77adc
Author: fperiott-eng <fperiott@ucm.es>
Date:   Fri Feb 20 15:59:18 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit efc464cbf8ddff8ad2723c1e5c304d484fd194c6[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Fri Feb 20 15:59:11 2026 +0100

    Create puntos_sinteticos.py
    
    Hecho por Sofia y Lautaro.
    Función que crea puntos sintéticos por cada incendio. Separado en máscaras e importancia del mismo. Puntos divididos en cercanos y aleatorios teniendo en cuenta el área y el número de incendios en esa zona. Variables con valor modificable para poder probar diferentes niveles de importancia o creación de puntos

[33mcommit df77adc5d1ed2fc530c1951480558120546949f9[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 15:56:03 2026 +0100

    Primer .py de datos no sintéticos
    
    Hecho por Lautaro y Sofía.
    Incluye funciones necesarias para crear datos sintéticos. Filtrar por zona, evaluar puntos válidos y crear fechas.
    Se subirán después las funciones que usan estas.

[33mcommit 9c571ff6c702e1d122fa91477bcfd1bcd1e1962b[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Fri Feb 20 11:19:53 2026 +0100

    Update main.py
    
    He parametrizado el main para poder probar y debuguear datos más específicos

[33mcommit 3b6099912cf2b70657dee31b75571b2506d5389a[m
Author: Sofia <sofcasad@ucm.es>
Date:   Fri Feb 20 10:39:26 2026 +0100

    Más cambios en este módulo - Sofía
    
    Veía necesario restaurar la función de obtenerNumero(lat, lon, src, transformer) para usarla con nuevos .py que añadiremos más tarde.

[33mcommit 34b0b6188ec9d621eaab6730ea90170471a905f3[m
Author: estebansueropf <esuero@ucm.es>
Date:   Fri Feb 20 08:42:53 2026 +0100

    Carga de datos directamente desde MinIO y pequeño cambio para importar las keys

[33mcommit 3791fb6f572e8b6e78db82924ae9d993759efeca[m
Author: Sofia <sofcasad@ucm.es>
Date:   Thu Feb 19 16:48:10 2026 +0100

    Inclusión de la variable vegetación 2 - Sofía
    
    Era necesario añadir al dataframe final la columna "vegetación 2". He hecho los cambios oportunos.

[33mcommit f353dd0ee3460d5b5b21c85aefdda72238f8758e[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Thu Feb 19 16:24:47 2026 +0100

    Update main
    
    He creado un menu para depuración y comprobación de datos por separado. También comprueba que esten todas las librerías importadas y comprueba que las rutas en .env funcionen. También puede hacer un pequeño analisis del trabajo para comprobar si tienes la versión de python adecuada y las rutas correctas

[33mcommit a557cdadb558f0ad530095683a97b20c73568816[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Thu Feb 19 13:10:05 2026 +0100

    Integrado en la función que crea un df completo vegetacion2

[33mcommit 0df6fd2224a7ac08028d9fdf14b12dd065e0a3be[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Thu Feb 19 12:51:06 2026 +0100

    Añadida función para extraer por separado el tipo de vegetación

[33mcommit 0f3b4cb942545e207b952f9aa943752d3f85dc1e[m
Author: estebansueropf <esuero@ucm.es>
Date:   Thu Feb 19 10:44:09 2026 +0100

    Añadido el paquete s3fs

[33mcommit 96d0c460b7ecca0df582208c9655ed3330a4e0c2[m
Author: estebansueropf <esuero@ucm.es>
Date:   Thu Feb 19 10:35:00 2026 +0100

    Añadido parametro scale al sacar los datos para evitar que devuelva nulo

[33mcommit c3c6d77dae65f8d0e04d37d1d81beef5771d1beb[m
Author: Sofia <sofcasad@ucm.es>
Date:   Thu Feb 19 10:03:58 2026 +0100

    Conexión con MinIO - Sofía
    
    Se trata de que se pueda leer el archivo .csv y .tif de vegetación sin necesidad de guardarlos en local.

[33mcommit e16af99eb41aae7978fa9b62f2f95158d42685a2[m
Author: estebansueropf <esuero@ucm.es>
Date:   Thu Feb 19 09:47:24 2026 +0100

    Cambios en el filtrado de nubes de vegetacion para usar el Cloud Score de Google

[33mcommit 0f68a52b582eef208ae763f2f5b8916b485318ac[m
Author: estebansueropf <esuero@ucm.es>
Date:   Wed Feb 18 19:32:03 2026 +0100

    Modificado .gitignore para no subir csv ni parquet por error

[33mcommit 72280d1f823bed5cd6d8f0e527699097c77fc2b1[m
Merge: c9a7751 065628b
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 19:28:01 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit c9a77513017b1a1f7b902a94b7f2352f726e3d28[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 19:27:54 2026 +0100

    Añadido fecha_ini y fecha_fin a la función fetch fires y al resto de las de extracción

[33mcommit 065628b2d04f1081d94f28d0d08cd5a76a0530f2[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Wed Feb 18 19:14:29 2026 +0100

    Automatizadas la creación de máscaras (biogeográficas y Europea) desde MinIO (raw) a formato geodataframe

[33mcommit cc4ce2f1064e55822c737b19d500fc721bf5c91c[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 18:21:27 2026 +0100

    Semáforos añadidos a algunas funciones async para evitar los errores de peticiones simultaneas

[33mcommit 93ff427679d3ef2ed2ec79713c0ca98e1142e12d[m
Merge: b230403 bdd6f88
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 18:04:31 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit b2304036fba98af46975c1d7e5fb415e42b3e9c8[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 18:04:26 2026 +0100

    Función para la extracción de las características físicas parcial añadida

[33mcommit bdd6f88275381fcae25f254cfcb5641d31396570[m
Author: aleexdz <aledie06@ucm.es>
Date:   Wed Feb 18 17:52:05 2026 +0100

    añadidas hectareas quemadas en fetch fires
    
    fetch fires devuelve tambien hectareas quemadas usando geopandas y shapely

[33mcommit 407de1d908e14feced9ad9de6811a709cb2c00b2[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 17:49:13 2026 +0100

    Función para la extracción de la pendiente parcial añadida

[33mcommit 8ddc05a4865dc6a02edfe751f8fae268946455af[m
Merge: 2cca4e3 d2167d2
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 17:40:35 2026 +0100

    Función para la extracción de vegetación parcial añadida

[33mcommit 2cca4e3e5607511602b41e88d90e135e237ec00b[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 17:39:30 2026 +0100

    Función para la extracción de vegetación parcial añadida

[33mcommit d2167d279192ac9c9edd86a3ae8e8a9c780a1e47[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Wed Feb 18 17:28:13 2026 +0100

    Funciones subir_fichero(), bajar_fichero() optimizadas con buffer en memoria

[33mcommit 64c8496593cc7828917e83415706a37dd6b359a7[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Wed Feb 18 17:11:57 2026 +0100

    Nombre cambiado para mayor claridad

[33mcommit 469f5e0969bea6652fc1a261f5157481b5c01ba9[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 18 16:54:41 2026 +0100

    Paralelización de llamadas a las apis para mayor eficiencia de la extracción

[33mcommit f0f2210946c70c7d2e755a252181ae3f5f3de4eb[m
Author: aleexdz <aledie06@ucm.es>
Date:   Tue Feb 17 17:49:35 2026 +0100

    construir_df ya es compatible con fetch fires
    
    he añadido al uv lock la dependencia sklearn, he modificado el archivo main.py para que podamos ejecutar todo desde ahi y he comentado mejor incendios.py

[33mcommit 17d02497c1233678c64ad47cdb20f2c361360d2c[m
Author: Sofia <sofcasad@ucm.es>
Date:   Tue Feb 17 16:04:53 2026 +0100

    Nuevas correcciones en los nombres
    
    También habían cambiado los nombres de los parámetros.

[33mcommit 5ef935d6ad7e583694863b4fb79e15954cae7183[m
Author: Sofia <sofcasad@ucm.es>
Date:   Tue Feb 17 16:01:59 2026 +0100

    Solución de errores
    
    Hemos detectado Alex y yo errores y los hemos resuelto. Era por un problema en la fecha.

[33mcommit 695c869aaae8bd70381f8a92efffd7645c5e939a[m
Author: Sofia <sofcasad@ucm.es>
Date:   Tue Feb 17 13:09:22 2026 +0100

    Cambios para optimizar tiempo de ejecución - Sofía
    
    He hecho cambios para que el mapa se abra una sola vez, lo que optimiza el tiempo de ejecución, pues antes se abría por consulta de un nuevo punto

[33mcommit 187a8a1080817ca075a4c1a6de744a2e3c13ab57[m
Author: aleexdz <aledie06@ucm.es>
Date:   Tue Feb 17 11:22:57 2026 +0100

    he comentado lo de incendios

[33mcommit a0b380e62d6cdb65adc9427c858842b241a8a374[m
Author: Sofia <sofcasad@ucm.es>
Date:   Mon Feb 16 22:56:15 2026 +0100

    Corrección pequeño error de identación

[33mcommit 0cef00da9c433e1ec2ee1eaef80df7e2ac814858[m
Author: Sofia <sofcasad@ucm.es>
Date:   Mon Feb 16 22:52:40 2026 +0100

    Cambios en vegetacion2 y construccion_df
    
    He cambiado los nombres de los archivos y he dado acceso de nuevo a la función de construcción, dado que ahora todos los datos van a ser de Europa, desde el principio de la extracción.

[33mcommit 8be0e6877435761a1fc97fbfa0d00606a3e3abcd[m
Merge: b3f461c 0f7bdfe
Author: aleexdz <aledie06@ucm.es>
Date:   Mon Feb 16 21:42:37 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit b3f461c4a497c4c36657748e30f5a140da580a06[m
Author: aleexdz <aledie06@ucm.es>
Date:   Mon Feb 16 21:42:29 2026 +0100

    recopilacion de datos de incendios

[33mcommit 0f7bdfe8f428531431c310a38029f862ebb618c8[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Mon Feb 16 21:35:15 2026 +0100

    Añadido minioFunctions.py para subir/bajar ficheros de minio

[33mcommit 0e5b98a261cc28a4af65ce35f793f4ee2c5dbab2[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Mon Feb 16 20:58:35 2026 +0100

    Update incendios.py modo csv
    
    Añadí una implementación para abrir los csv, extraer los datos y filtrar para que queden incendios únicos. Dejo la función anterior comentada.

[33mcommit 41db49439b547bddb7b2ef7d08b3aba9a1fca99e[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Mon Feb 16 16:40:11 2026 +0100

    Añadido parquet.py para procesar parquets y modificado mascaras.py

[33mcommit e9e7956bdbbc4b0aad030ce4d480ad283bac8fb9[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Mon Feb 16 15:45:10 2026 +0100

    limpieza código construcción_df
    
    Sofia y yo lo limpiamos un poco

[33mcommit 2ac23a39412bad7a53671484ad54f2fe72030938[m
Author: fperiott-eng <fperiott@ucm.es>
Date:   Mon Feb 16 15:42:26 2026 +0100

    incendios.py parametrizado
    
    Aquí Sofia y yo hemos parametrizado la función para poder repartir el trabajo de búsqueda

[33mcommit 209a0e22f550bc9e2cccfd4c0e2cc5016e7de2ef[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Fri Feb 13 20:52:55 2026 +0100

    Implementadas funciones para extraer desde .parquet y comprobar pertenencia de un punto en una máscara

[33mcommit 9eedc913125187d97aaaedbdf7d3057d7495e98b[m
Author: TebanQuito <estebansuerobadajoz@gmail.com>
Date:   Fri Feb 13 15:46:42 2026 +0100

    Comentado vegetacion.py

[33mcommit f6f9f120685ab055b259c3d3b2efcb6484b55487[m
Author: Ignaciiovallejo <ignavall@ucm.es>
Date:   Thu Feb 12 22:12:15 2026 +0100

    Creado archivo mascaras.py con mascaras para europa y para sus bioregiones

[33mcommit 7e1da29db3139ba0270683a37b553f9802ee0fd6[m
Merge: f2a0011 d279efc
Author: JuananAlonso <juananal@ucm.es>
Date:   Thu Feb 12 17:43:06 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3

[33mcommit f2a0011da70fed8f12db1744d13db50dc69352f5[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Thu Feb 12 17:42:51 2026 +0100

    pendiente.py comentado

[33mcommit d279efc22bc971efda579fbbadf15c5fdb789071[m
Author: aleexdz <aledie06@ucm.es>
Date:   Thu Feb 12 17:36:11 2026 +0100

    he añadido info sobre: intensidad.py

[33mcommit 437cd80173b92ec94c1061709a20f7aa3eeefcab[m
Author: Sofia <sofcasad@ucm.es>
Date:   Thu Feb 12 17:31:38 2026 +0100

    Update vegetacion2.py

[33mcommit 52ab6947403b05892657cb6a2548893849b38419[m
Author: Sofia <sofcasad@ucm.es>
Date:   Thu Feb 12 17:18:58 2026 +0100

    Update vegetacion2.py

[33mcommit 2ecc316b38ae40d43944e06c2b784812bd930538[m
Author: TebanQuito <estebansuerobadajoz@gmail.com>
Date:   Wed Feb 11 20:45:06 2026 +0100

    Eliminacion de variable que no hacia nada en vegetacion.py

[33mcommit 71c6b9906c862e81c2241008b7f5522a195a8029[m
Author: TebanQuito <estebansuerobadajoz@gmail.com>
Date:   Wed Feb 11 20:36:24 2026 +0100

    Cambio en la autenticación de Google Earth Engine para la vegetacion

[33mcommit cb0fbf7f985afef723466137931e2a6771d6d835[m
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 11 17:25:47 2026 +0100

    Notebooks transformados a .py y modularizados (el codigo no es solo mío) & carpeta extracción creada

[33mcommit 32c4dd9375da8174f5e3b6181bfc7178305f96e7[m
Merge: 6b3668c 260c7ba
Author: JuananAlonso <juananal@ucm.es>
Date:   Wed Feb 11 16:15:57 2026 +0100

    Merge branch 'main' of https://github.com/UCM-GIDIA-PD1/c2526-R3
