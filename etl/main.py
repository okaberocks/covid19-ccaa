"""Implements ETL processing for COVID-19 datasets.

It performs the following actions:

1. pull updated datasets from https://github.com/datadista/datasets
 1.1. SOURCE environment variable points to a local repository path
2. read .csv data files into pandas dataframes
3. export data to JSONStat format
4. push JSON files to gist

"""

"""Import configuration."""
from config import etl_cfg

from etlstat.extractor.extractor import csv

from git import GitCommandError, Repo

from pyjstat import pyjstat


def normalize(df, variable):
    """Normalize and filter regional data."""
    df.drop(df[df.cod_ine != 6].index, inplace=True)
    df.drop('cod_ine', axis=1, inplace=True)
    df.set_index('CCAA')
    df = df.melt(id_vars=['CCAA'], var_name='fecha')
    df.drop('CCAA', axis=1, inplace=True)
    df.rename(columns={'value': variable}, inplace=True)
    return df

def transform(df, variable):
    """Filter rows and drop columns."""
    df.drop(df[df.cod_ine != 6].index, inplace=True)
    df.reset_index(inplace=True)
    df.drop('index', axis=1, inplace=True)
    df.drop('cod_ine', axis=1, inplace=True)
    df.drop('CCAA', axis=1, inplace=True)
    df.rename(columns={'total': variable}, inplace=True)
    return df

def deacumulate(df, variable1, variable2):
    for i in range(1, len(df)):
        df.loc[i, variable2] = df.loc[i, variable1] - \
        df.loc[i-1, variable1]
    return df

def to_json(df, id_vars, value_vars):
    """Export dataframe to JSON-Stat dataset.
    
        id_vars (list): index columns
        value_vars (list): numeric variables (metrics)
    """
    df = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='Variables')
    id_vars.append('Variables')
    df = df.sort_values(by=id_vars)
    dataset = pyjstat.Dataset.read(df, source=etl_cfg.metadata.source)
    metric = {'metric': ['Variables']}
    dataset.setdefault('role', metric)
    return dataset.write(output='jsonstat')

def write_to_file(json_data, file_name):
    file = open(file_name, 'w')
    file.write(json_data)
    file.close()


"""First step: pull data from Github repository."""
repo = Repo(etl_cfg.input.source)
o = repo.remotes.origin
o.pull()

"""Second step: load .csv data files into dataframes."""
data = csv(etl_cfg.input.dir_path, sep=',')

"""Third step: ETL processing."""
# Datos nacionales acumulados diarios
# fecha,casos,altas,fallecimientos,ingresos_uci,hospitalizados
nacional = data[etl_cfg.input.files.nacional]
nacional.set_index('fecha')
nacional.rename(columns={
    'casos': 'casos-acumulado',
    'altas': 'altas-acumulado',
    'fallecimientos': 'fallecidos-acumulado',
    'ingresos_uci': 'uci-acumulado',
    'hospitalizados': 'hospital-acumulado'},
    inplace=True)
# Calcular datos diarios no acumulados
for i in range(1, len(nacional)):
    nacional.loc[i, 'casos'] = nacional.loc[i, 'casos-acumulado'] - \
    nacional.loc[i-1, 'casos-acumulado']
    nacional.loc[i, 'altas'] = nacional.loc[i, 'altas-acumulado'] - \
    nacional.loc[i-1, 'altas-acumulado']
    nacional.loc[i, 'fallecidos'] = nacional.loc[i, 'fallecidos-acumulado'] - \
    nacional.loc[i-1, 'fallecidos-acumulado']
    nacional.loc[i, 'uci'] = nacional.loc[i, 'uci-acumulado'] - \
    nacional.loc[i-1, 'uci-acumulado']
    nacional.loc[i, 'hospital'] = nacional.loc[i, 'hospital-acumulado'] - \
    nacional.loc[i-1, 'hospital-acumulado']

# Datos acumulados
nacional_acumulado = nacional[[
    'fecha',
    'casos-acumulado',
    'altas-acumulado',
    'fallecidos-acumulado',
    'uci-acumulado',
    'hospital-acumulado']].copy()
nacional_acumulado.rename(
    columns={
        'casos-acumulado': 'casos',
        'altas-acumulado': 'altas',
        'fallecidos-acumulado': 'fallecidos',
        'uci-acumulado': 'uci',
        'hospital-acumulado': 'hospital'},
        inplace=True)
json_file = to_json(
    nacional_acumulado,
    ['fecha'],
    ['casos', 'altas', 'fallecidos', 'uci', 'hospital'])
write_to_file(json_file, etl_cfg.output.path + 'todos_nacional_acumulado.json-stat')

# Datos diarios
nacional_diario = nacional[[
    'fecha',
    'casos',
    'altas',
    'fallecidos',
    'uci',
    'hospital']].copy()
json_file = to_json(
    nacional_diario,
    ['fecha'],
    ['casos', 'altas', 'fallecidos', 'uci', 'hospital'])
write_to_file(json_file, etl_cfg.output.path + 'todos_nacional_diario.json-stat')

# Datos nacionales por rango de edad y sexo
nacional_edad = data[etl_cfg.input.files.nacional_edad]
nacional_edad.drop(nacional_edad[nacional_edad.rango_edad == 'Total'].index, inplace=True)
nacional_edad.drop(nacional_edad[nacional_edad.sexo == 'ambos'].index, inplace=True)
last_date = nacional_edad['fecha'].max()
nacional_edad.drop(nacional_edad[nacional_edad.fecha != last_date].index, inplace=True)
nacional_edad.drop('fecha', axis=1, inplace=True)
nacional_edad.rename(columns={
    'casos_confirmados': 'casos',
    'hospitalizados': 'hospital',
    'ingresos_uci': 'uci'
}, inplace=True)

nacional_edad_casos = nacional_edad[['rango_edad', 'sexo', 'casos']].copy()
json_file = to_json(nacional_edad_casos, ['rango_edad', 'sexo'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_nacional_edad_sexo.json-stat')

nacional_edad_hospital = nacional_edad[['rango_edad', 'sexo', 'hospital']].copy()
json_file = to_json(nacional_edad_hospital, ['rango_edad', 'sexo'], ['hospital'])
write_to_file(json_file, etl_cfg.output.path + 'hospital_nacional_edad_sexo.json-stat')

nacional_edad_uci = nacional_edad[['rango_edad', 'sexo', 'uci']].copy()
json_file = to_json(nacional_edad_uci, ['rango_edad', 'sexo'], ['uci'])
write_to_file(json_file, etl_cfg.output.path + 'uci_nacional_edad_sexo.json-stat')

nacional_edad_fallecidos = nacional_edad[['rango_edad', 'sexo', 'fallecidos']].copy()
json_file = to_json(nacional_edad_fallecidos, ['rango_edad', 'sexo'], ['fallecidos'])
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_nacional_edad_sexo.json-stat')

# Casos en Cantabria
# fecha,cod_ine,CCAA,total
casos = data[etl_cfg.input.files.casos]
casos = transform(casos, 'casos-acumulado')
# cifra más reciente
casos_last = casos.tail(1)
casos_last.rename(columns={'casos-acumulado': 'casos'}, inplace=True)
json_file = to_json(casos_last, ['fecha'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_1_dato.json-stat')

casos = deacumulate(casos, 'casos-acumulado', 'casos')

# acumulado
casos_acumulado = casos[['fecha', 'casos-acumulado']].copy()
casos_acumulado.rename(columns={'casos-acumulado': 'casos'}, inplace=True)
json_file = to_json(casos_acumulado, ['fecha'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_acumulado.json-stat')

# diario
casos_diario = casos[['fecha', 'casos']].copy()
json_file = to_json(casos_diario, ['fecha'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_diario.json-stat')

# tasa de variación diaria (porcentaje)
# T(d) = 100 * ((Casos(d) - Casos(d-1))/Casos(d-1))
casos_tasa = casos_diario
for i in range(1, len(casos_tasa)):
    if casos_tasa.loc[i-1, 'casos'] > 0:
        casos_tasa.loc[i, 'variacion'] = 100 * (( \
            casos_tasa.loc[i, 'casos'] - casos_tasa.loc[i-1, 'casos']) / \
            casos_tasa.loc[i-1, 'casos'])
    else:
        casos_tasa.loc[i, 'variacion'] = None
json_file = to_json(casos_tasa, ['fecha'], ['casos', 'variacion'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_variacion.json-stat')

# Altas en Cantabria
# fecha,cod_ine,CCAA,total
altas = data[etl_cfg.input.files.altas]
altas = transform(altas, 'altas-acumulado')
# cifra más reciente
altas_last = altas.tail(1)
altas_last.rename(columns={'altas-acumulado': 'altas'}, inplace=True)
json_file = to_json(altas_last, ['fecha'], ['altas'])
write_to_file(json_file, etl_cfg.output.path + 'altas_cantabria_1_dato.json-stat')

altas = deacumulate(altas, 'altas-acumulado', 'altas')

# acumulado
altas_acumulado = altas[['fecha', 'altas-acumulado']].copy()
altas_acumulado.rename(columns={'altas-acumulado': 'altas'}, inplace=True)
json_file = to_json(altas_acumulado, ['fecha'], ['altas'])
write_to_file(json_file, etl_cfg.output.path + 'altas_cantabria_acumulado.json-stat')

# diario
altas_diario = altas[['fecha', 'altas']].copy()
json_file = to_json(altas_diario, ['fecha'], ['altas'])
write_to_file(json_file, etl_cfg.output.path + 'altas_cantabria_diario.json-stat')

# Ingresados en UCI en Cantabria
# fecha,cod_ine,CCAA,total
uci = data[etl_cfg.input.files.uci]
uci = transform(uci, 'uci-acumulado')
# cifra más reciente
uci_last = uci.tail(1)
uci_last.rename(columns={'uci-acumulado': 'uci'}, inplace=True)
json_file = to_json(uci_last, ['fecha'], ['uci'])
write_to_file(json_file, etl_cfg.output.path + 'uci_cantabria_1_dato.json-stat')

uci = deacumulate(uci, 'uci-acumulado', 'uci')

# acumulado
uci_acumulado = uci[['fecha', 'uci-acumulado']].copy()
uci_acumulado.rename(columns={'uci-acumulado': 'uci'}, inplace=True)
json_file = to_json(uci_acumulado, ['fecha'], ['uci'])
write_to_file(json_file, etl_cfg.output.path + 'uci_cantabria_acumulado.json-stat')

# diario
uci_diario = uci[['fecha', 'uci']].copy()
json_file = to_json(uci_diario, ['fecha'], ['uci'])
write_to_file(json_file, etl_cfg.output.path + 'uci_cantabria_diario.json-stat')

# Fallecidos en Cantabria
# fecha,cod_ine,CCAA,total
fallecidos = data[etl_cfg.input.files.fallecidos]
fallecidos = transform(fallecidos, 'fallecidos-acumulado')
# cifra más reciente
fallecidos_last = fallecidos.tail(1)
fallecidos_last.rename(columns={'fallecidos-acumulado': 'fallecidos'}, inplace=True)
json_file = to_json(fallecidos_last, ['fecha'], ['fallecidos'])
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_cantabria_1_dato.json-stat')

fallecidos = deacumulate(fallecidos, 'fallecidos-acumulado', 'fallecidos')

# acumulado
fallecidos_acumulado = fallecidos[['fecha', 'fallecidos-acumulado']].copy()
fallecidos_acumulado.rename(columns={'fallecidos-acumulado': 'fallecidos'}, inplace=True)
json_file = to_json(fallecidos_acumulado, ['fecha'], ['fallecidos'])
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_cantabria_acumulado.json-stat')

# diario
fallecidos_diario = fallecidos[['fecha', 'fallecidos']].copy()
json_file = to_json(fallecidos_diario, ['fecha'], ['fallecidos'])
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_cantabria_diario.json-stat')

# Todas las variables acumulado en Cantabria
todas_acumulado = casos.merge(altas, how='left', on='fecha')
todas_acumulado = todas_acumulado.merge(fallecidos, how='left', on='fecha')
todas_acumulado = todas_acumulado.merge(uci, how='left', on='fecha')
todas_acumulado.drop('casos', axis=1, inplace=True)
todas_acumulado.drop('altas', axis=1, inplace=True)
todas_acumulado.drop('fallecidos', axis=1, inplace=True)
todas_acumulado.drop('uci', axis=1, inplace=True)
todas_acumulado.rename(columns={
    'casos-acumulado': 'casos',
    'altas-acumulado': 'altas',
    'fallecidos-acumulado': 'fallecidos',
    'uci-acumulado': 'uci'}, inplace=True)
json_file = to_json(
    todas_acumulado,
    ['fecha'],
    ['casos', 'altas', 'fallecidos', 'uci'])
write_to_file(json_file, etl_cfg.output.path + 'todos_cantabria.json-stat')

# Comparación casos Cantabria y España
espana = data[etl_cfg.input.files.nacional]
cant_esp = espana.merge(casos, how='left', on='fecha')
cant_esp.drop('altas-acumulado', axis=1, inplace=True)
cant_esp.drop('fallecidos-acumulado', axis=1, inplace=True)
cant_esp.drop('uci-acumulado', axis=1, inplace=True)
cant_esp.drop('hospital-acumulado', axis=1, inplace=True)
cant_esp.drop('casos_x', axis=1, inplace=True)
cant_esp.drop('altas', axis=1, inplace=True)
cant_esp.drop('fallecidos', axis=1, inplace=True)
cant_esp.drop('uci', axis=1, inplace=True)
cant_esp.drop('hospital', axis=1, inplace=True)
cant_esp.drop('casos_y', axis=1, inplace=True)
cant_esp.rename(columns={
    'casos-acumulado_x': 'casos-espana',
    'casos-acumulado_y': 'casos-cantabria'}, inplace=True)
json_file = to_json(cant_esp, ['fecha'], ['casos-espana', 'casos-cantabria'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_espana.json-stat')

"""Fourth step: push JSON-Stat files to repository."""
repo = Repo(etl_cfg.output.repository)
repo.git.add('--all')
try:
    repo.git.commit('-m', '"Automatic update"')
    origin = repo.remote(name='origin')
    origin.push()
except GitCommandError:
    pass
