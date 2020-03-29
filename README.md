# covid19-ccaa
Script en lenguaje Python para procesar datos nacionales y regionales sobre la epidemia de COVID19.

Datos fuente proporcionados por https://datadista.com

## Variables de entorno

La variable SOURCE environment almacena la ruta local del repositorio  https://github.com/datadista/datasets


Este repositorio proporciona datos diarios actualizados sobre la evolución de la epidemia de COVID19 en España y Cantabria, en formato JSON-Stat.


## Ficheros de resultados

En etl/data:

+ **todos_nacional_acumulado.json-stat** -> Datos acumulados: 'fecha', 'casos', 'altas', 'fallecidos', 'uci', 'hospital'
+ **todos_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'casos', 'altas', 'fallecidos', 'uci', 'hospital'
+ **casos_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'casos'
+ **hospital_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'hospital'
+ **uci_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'uci'
+ **fallecidos_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'fallecidos'
+ **casos_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'casos'
+ **casos_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'casos'
+ **casos_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'casos'
+ **casos_cantabria_variacion.json-stat** -> Tasa de variación diaria, en porcentaje: 'fecha', 'casos', 'variacion'
+ **altas_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'altas'
+ **altas_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'altas'
+ **altas_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'altas'
+ **uci_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'uci'
+ **uci_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'uci'
+ **uci_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'uci'
+ **fallecidos_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'fallecidos'
+ **fallecidos_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'fallecidos'
+ **fallecidos_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'fallecidos'
+ **todos_cantabria.json-stat** -> Datos acumulados: 'fecha', 'casos',     'altas', 'fallecidos', 'uci'
+ **casos_cantabria_espana.json-stat** -> Datos acumulados: 'fecha', 'casos-espana', 'casos-cantabria'