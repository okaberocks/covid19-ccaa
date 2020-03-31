# covid19-ccaa
Script en lenguaje Python para procesar datos nacionales y regionales sobre la epidemia de COVID19.

Datos fuente proporcionados por https://datadista.com

## Variables de entorno

La variable SOURCE environment almacena la ruta local del repositorio  https://github.com/datadista/datasets


Este repositorio proporciona datos diarios actualizados sobre la evolución de la epidemia de COVID19 en España y Cantabria, en formato JSON-Stat.


## Ficheros de resultados

En etl/data:

+ Datos nacionales
 + **todos_nacional_acumulado.json-stat** -> Datos acumulados: 'fecha', 'casos', 'altas', 'fallecidos', 'uci', 'hospital'
 + **todos_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'casos', 'altas', 'fallecidos', 'uci', 'hospital'
 + **casos_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'casos'
 + **fallecidos_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'fallecidos'
 + **hospital_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'hospital'
 + **uci_nacional_edad_sexo.json-stat** -> Dato más reciente: 'rango_edad', 'sexo', 'uci'
 + **altas_nacional_1_dato.json-stat** -> Dato más reciente: 'fecha', 'altas'
 + **altas_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'altas'
 + **casos_nacional_1_dato.json-stat** -> Dato más reciente: 'fecha', 'casos'
 + **casos_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'casos'
 + **casos_nacional_variacion.json-stat** -> Datos acumulados: 'fecha', 'variacion'
 + **fallecidos_nacional_1_dato.json-stat** -> Dato más reciente: 'fecha', 'fallecidos'
 + **fallecidos_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'fallecidos'
 + **hospital_nacional_1_dato.json-stat** -> Dato más reciente: 'fecha', 'hospital'
 + **hospital_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'uci'
 + **uci_nacional_1_dato.json-stat** -> Dato más reciente: 'fecha', 'uci'
 + **uci_nacional_diario.json-stat** -> Datos diarios: 'fecha', 'uci'
+ CA de Cantabria
 + **altas_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'altas'
 + **altas_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'altas'
 + **altas_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'altas'
 + **casos_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'casos'
 + **casos_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'casos'
 + **casos_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'casos'
 + **casos_cantabria_espana.json-stat** -> Datos acumulados: 'fecha', 'casos-espana', 'casos-cantabria'
 + **casos_cantabria_variacion.json-stat** -> Tasa de variación diaria, en porcentaje: 'fecha', 'variacion'
 + **fallecidos_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'fallecidos'
 + **fallecidos_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'fallecidos'
 + **fallecidos_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'fallecidos'
 + **uci_cantabria_1_dato.json-stat** -> Dato más reciente: 'fecha', 'uci'
 + **uci_cantabria_acumulado.json-stat** -> Datos acumulados: 'fecha', 'uci'
 + **uci_cantabria_diario.json-stat** -> Datos diarios: 'fecha', 'uci'
 + **todos_cantabria.json-stat** -> Datos acumulados: 'fecha', 'casos', 'altas', 'fallecidos', 'uci'
+ Datos por comunidades autónomas
 + **todos_ccaa_acumulado.json-stat** -> Datos acumulados: 'fecha', 'ccaa', 'altas', 'casos', 'fallecidos', 'hospital', 'uci'