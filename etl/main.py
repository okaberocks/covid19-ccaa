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

from git import Repo

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

def to_json_stat(df, variable1, variable2):
    """Export dataframe to JSON-Stat dataset."""
    df = df.melt(
        id_vars=['fecha'],
        value_vars=[variable1, variable2],
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
json = dataset.write(output='jsonstat')
write_to_file(json, etl_cfg.output.path + 'todos_nacional.json-stat')

# Casos en Cantabria
# fecha,cod_ine,CCAA,total
casos = data[etl_cfg.input.files.casos]
casos = transform(casos, 'casos-acumulado')
casos = deacumulate(casos, 'casos-acumulado', 'casos')
json = to_json_stat(casos, 'casos-acumulado', 'casos')
write_to_file(json, etl_cfg.output.path + 'casos_cantabria.json-stat')

# Altas en Cantabria
# fecha,cod_ine,CCAA,total
altas = data[etl_cfg.input.files.altas]
altas = transform(altas, 'altas-acumulado')
altas = deacumulate(altas, 'altas-acumulado', 'altas')
json = to_json_stat(altas, 'altas-acumulado', 'altas')
write_to_file(json, etl_cfg.output.path + 'altas_cantabria.json-stat')

# Ingresados en UCI en Cantabria
# fecha,cod_ine,CCAA,total
uci = data[etl_cfg.input.files.uci]
uci = transform(uci, 'uci-acumulado')
uci = deacumulate(uci, 'uci-acumulado', 'uci')
json = to_json_stat(uci, 'uci-acumulado', 'uci')
write_to_file(json, etl_cfg.output.path + 'uci_cantabria.json-stat')

# Número de fallecidos por CCAA
# fecha,cod_ine,CCAA,total
fallecidos = data[etl_cfg.input.files.fallecidos]
fallecidos = transform(fallecidos, 'fallecidos-acumulado')
fallecidos = deacumulate(fallecidos, 'fallecidos-acumulado', 'fallecidos')
json = to_json_stat(fallecidos, 'fallecidos-acumulado', 'fallecidos')
write_to_file(json, etl_cfg.output.path + 'fallecidos_cantabria.json-stat')
