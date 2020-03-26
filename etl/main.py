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

def to_json_stat(df, variable1, variable2=None):
    """Export dataframe to JSON-Stat dataset."""
    if variable2:
        df = df.melt(
            id_vars=['fecha'],
            value_vars=[variable1, variable2],
            var_name='Variables')
    else:
        df = df.melt(
            id_vars=['fecha'],
            value_vars=[variable1],
            var_name='Variables')
    df = df.sort_values(by=['fecha', 'Variables'])
    dataset = pyjstat.Dataset.read(df)
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
# Preparación del dataset
nacional = nacional.melt(
    id_vars=['fecha'],
    value_vars=[
        'casos-acumulado', 'altas-acumulado', 'fallecidos-acumulado',
        'uci-acumulado', 'hospital-acumulado', 'casos', 'altas',
        'fallecidos', 'uci', 'hospital'],
    var_name='Variables')
nacional = nacional.sort_values(by=['fecha', 'Variables'])
dataset = pyjstat.Dataset.read(nacional)
metric = {'metric': ['Variables']}
dataset.setdefault('role', metric)
json_file = dataset.write(output='jsonstat')
write_to_file(json_file, etl_cfg.output.path + 'todos_nacional.json-stat')

# Casos en Cantabria
# fecha,cod_ine,CCAA,total
casos = data[etl_cfg.input.files.casos]
casos = transform(casos, 'casos-acumulado')
# cifra más reciente
casos_last = casos.tail(1)
casos_last.rename(columns={'casos-acumulado': 'casos'}, inplace=True)
json_file = to_json_stat(casos_last, 'casos')
write_to_file(json_file, etl_cfg.output.path + 'casos_1_dato.json-stat')
# diario y acumulado
casos = deacumulate(casos, 'casos-acumulado', 'casos')
json_file = to_json_stat(casos, 'casos-acumulado', 'casos')
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria.json-stat')
# sólo diario
casos_diario = casos
casos_diario = casos_diario.drop('casos-acumulado', axis=1)
json_file = to_json_stat(casos_diario, 'casos')
write_to_file(json_file, etl_cfg.output.path + 'casos_diarios_cantabria.json-stat')

# Altas en Cantabria
# fecha,cod_ine,CCAA,total
altas = data[etl_cfg.input.files.altas]
altas = transform(altas, 'altas-acumulado')
# cifra más reciente
altas_last = altas.tail(1)
altas_last.rename(columns={'altas-acumulado': 'altas'}, inplace=True)
json_file = to_json_stat(altas_last, 'altas')
write_to_file(json_file, etl_cfg.output.path + 'altas_1_dato.json-stat')
# diario y acumulado
altas = deacumulate(altas, 'altas-acumulado', 'altas')
json_file = to_json_stat(altas, 'altas-acumulado', 'altas')
write_to_file(json_file, etl_cfg.output.path + 'altas_cantabria.json-stat')

# Ingresados en UCI en Cantabria
# fecha,cod_ine,CCAA,total
uci = data[etl_cfg.input.files.uci]
uci = transform(uci, 'uci-acumulado')
# cifra más reciente
uci_last = uci.tail(1)
uci_last.rename(columns={'uci-acumulado': 'uci'}, inplace=True)
json_file = to_json_stat(uci_last, 'uci')
write_to_file(json_file, etl_cfg.output.path + 'uci_1_dato.json-stat')
# diario y acumulado
uci = deacumulate(uci, 'uci-acumulado', 'uci')
json_file = to_json_stat(uci, 'uci-acumulado', 'uci')
write_to_file(json_file, etl_cfg.output.path + 'uci_cantabria.json-stat')

# Fallecidos en Cantabria
# fecha,cod_ine,CCAA,total
fallecidos = data[etl_cfg.input.files.fallecidos]
fallecidos = transform(fallecidos, 'fallecidos-acumulado')
# cifra más reciente
fallecidos_last = fallecidos.tail(1)
fallecidos_last.rename(columns={'fallecidos-acumulado': 'fallecidos'}, inplace=True)
json_file = to_json_stat(fallecidos_last, 'fallecidos')
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_1_dato.json-stat')
# diario y acumulado
fallecidos = deacumulate(fallecidos, 'fallecidos-acumulado', 'fallecidos')
json_file = to_json_stat(fallecidos, 'fallecidos-acumulado', 'fallecidos')
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_cantabria.json-stat')

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
todas_acumulado = todas_acumulado.melt(
    id_vars=['fecha'],
    value_vars=[
        'casos', 'altas',
        'fallecidos', 'uci'],
    var_name='Variables')
todas_acumulado = todas_acumulado.sort_values(by=['fecha', 'Variables'])
dataset = pyjstat.Dataset.read(todas_acumulado)
metric = {'metric': ['Variables']}
dataset.setdefault('role', metric)
json_file = dataset.write(output='jsonstat')
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
json_file = to_json_stat(cant_esp, 'casos-espana', 'casos-cantabria')
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
