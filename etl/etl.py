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
    
def to_json_stat(df, variable):
    """Export dataframe to JSON-Stat dataset."""
    df = df.melt(
        id_vars=['fecha'],
        value_vars=[variable],
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
# Datos nacionales
nacional = data[etl_cfg.input.files.nacional]
nacional.set_index('fecha')
nacional = nacional.melt(
    id_vars=['fecha'],
    value_vars=[
        'casos', 'altas', 'fallecimientos',
        'ingresos_uci', 'hospitalizados'],
    var_name='Variables')
nacional = nacional.sort_values(by=['fecha', 'Variables'])
dataset = pyjstat.Dataset.read(nacional)
metric = {'metric': ['Variables']}
dataset.setdefault('role', metric)
json = dataset.write(output='jsonstat')
write_to_file(json, etl_cfg.output.path + 'todos_espana.json-stat')

# Casos en Cantabria
casos = data[etl_cfg.input.files.casos]
casos = normalize(casos, 'casos-diario')
json = to_json_stat(casos, 'casos-diario')
write_to_file(json, etl_cfg.output.path + 'casos_diarios_cantabria.json-stat')

# Altas en Cantabria
altas = data[etl_cfg.input.files.altas]
altas = normalize(altas, 'altas-diario')
json = to_json_stat(altas, 'altas-diario')
write_to_file(json, etl_cfg.output.path + 'altas_diarias_cantabria.json-stat')

# Ingresados en UCI en Cantabria
uci = data[etl_cfg.input.files.uci]
uci = normalize(uci, 'uci-diario')
json = to_json_stat(uci, 'uci-diario')
write_to_file(json, etl_cfg.output.path + 'uci_diarios_cantabria.json-stat')

# Número de fallecidos por CCAA
fallecidos = data[etl_cfg.input.files.fallecidos]
fallecidos = normalize(fallecidos, 'fallecidos-diario')
json = to_json_stat(fallecidos, 'fallecidos-diario')
write_to_file(json, etl_cfg.output.path + 'fallecidos_diarios_cantabria.json-stat')
