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

from datetime import datetime, timedelta

from etlstat.extractor.extractor import csv

from git import GitCommandError, Repo

import json

from numpy import arange

from pyjstat import pyjstat


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

def normalize_ccaa(df, variable):
    """Rename and drop columns."""
    df_new = df.rename(
        columns={'CCAA': 'ccaa', 'total': variable})
    df_new.drop('cod_ine', axis=1, inplace=True)
    df_new.drop(df_new[df_new.ccaa == 'Total'].index, inplace=True)
    df_new.set_index('fecha', 'ccaa')
    return df_new

def delay_date(df):
    """Change dates to previous day."""
    for i in range(0, len(df)):
        delay_date = datetime.strptime(df.loc[i, 'fecha'], '%Y-%m-%d')
        delay_date = delay_date - timedelta(days=1)
        delay_date_str = delay_date.strftime('%Y-%m-%d')
        df.loc[i, 'fecha'] = delay_date_str
    return df

"""First step: pull data from Github repository."""
repo = Repo(etl_cfg.input.source)
o = repo.remotes.origin
o.pull()

"""Second step: load .csv data files into dataframes."""
data = csv(etl_cfg.input.dir_path, sep=',')

"""Third step: ETL processing."""
# Estaciones de servicio
eess = data[etl_cfg.input.files.eess]
eess.rename(
    columns = {
        'Horario': 'horario',
        'Provincia': 'provincia',
        'Municipio': 'municipio',
        'Código\nPostal': 'codigo_postal',
        'Dirección': 'direccion',
        'Margen': 'margen',
        'Rótulo': 'rotulo'
    }, inplace=True)
eess['id'] = arange(len(eess))
eess['Latitud'] = eess['Latitud'].str.replace(',','.')
eess['Longitud'] = eess['Longitud'].str.replace(',','.')
json_file = to_json(
    eess,
    ['id'],
    ['horario', 'provincia', 'municipio',
     'codigo_postal', 'direccion', 'Latitud', 'Longitud',
     'margen', 'rotulo'])
write_to_file(json_file, etl_cfg.output.path + 'eess_horario_flexible_habitual.json-stat')

# Puntos de restauración
restauracion = data[etl_cfg.input.files.restauracion]
restauracion.rename(
    columns = {
        'NOMBRE': 'nombre',
        'Tipo': 'tipo',
        'Direccion': 'direccion',
        'Municipio': 'municipio',
        'Provincia': 'provincia',
        'Comentarios': 'comentario',
        'Horario': 'horario',
        'Telefono': 'telefono',
        'Bocata_Bebida_Caliente': 'bocadillo_bebida_caliente',
        'Comida_Preparada': 'comida_preparada',
        'Ducha': 'ducha'
    }, inplace=True)
restauracion['id'] = arange(len(restauracion))
json_file = to_json(
    restauracion,
    ['id'],
    ['nombre', 'tipo', 'direccion', 'municipio',
     'provincia', 'Latitud', 'Longitud', 'comentario',
     'horario', 'telefono', 'bocadillo_bebida_caliente',
     'comida_preparada', 'ducha'])
write_to_file(json_file, etl_cfg.output.path + 'puntos_restauracion.json-stat')

# Alojamientos turísticos BOE 2020 4194
alojamientos = data[etl_cfg.input.files.alojamientos]
alojamientos.rename(
    columns={'CCAA': 'ccaa', 'lat': 'Latitud', 'long': 'Longitud'},
    inplace=True)
alojamientos.loc[
    (alojamientos.provincia == 'Santander'), 'provincia'] = 'Cantabria'
alojamientos['id'] = arange(len(alojamientos))
json_file = to_json(
    alojamientos,
    ['id'],
    ['ccaa', 'provincia', 'localidad', 'nombre', 'Latitud', 'Longitud'])
write_to_file(json_file, etl_cfg.output.path + 'alojamientos_turisticos.json-stat')

# Datos nacionales acumulados, por comunidad autónoma
ccaa_altas = data[etl_cfg.input.files.altas]
ccaa_altas = normalize_ccaa(ccaa_altas, 'altas')
ccaa_altas = delay_date(ccaa_altas)
ccaa_casos = data[etl_cfg.input.files.casos]
ccaa_casos = normalize_ccaa(ccaa_casos, 'casos')
ccaa_casos = delay_date(ccaa_casos)
ccaa_fallecidos = data[etl_cfg.input.files.fallecidos]
ccaa_fallecidos = normalize_ccaa(ccaa_fallecidos, 'fallecidos')
ccaa_fallecidos = delay_date(ccaa_fallecidos)
ccaa_hospital = data[etl_cfg.input.files.hospital]
ccaa_hospital = normalize_ccaa(ccaa_hospital, 'hospital')
ccaa_hospital = delay_date(ccaa_hospital)
ccaa_uci = data[etl_cfg.input.files.uci]
ccaa_uci = normalize_ccaa(ccaa_uci, 'uci')
ccaa_uci = delay_date(ccaa_uci)
todos_ccaa = ccaa_casos.merge(ccaa_altas, how='left', on=['fecha', 'ccaa'])
todos_ccaa = todos_ccaa.merge(ccaa_fallecidos, how='left', on=['fecha', 'ccaa'])
todos_ccaa = todos_ccaa.merge(ccaa_hospital, how='left', on=['fecha', 'ccaa'])
todos_ccaa = todos_ccaa.merge(ccaa_uci, how='left', on=['fecha', 'ccaa'])
json_file = to_json(
    todos_ccaa,
    ['fecha', 'ccaa'],
    ['casos', 'altas', 'fallecidos', 'hospital', 'uci'])
write_to_file(json_file, etl_cfg.output.path + 'todos_ccaa_acumulado.json-stat')
# Cifras más recientes, por CCAA
last_date = todos_ccaa['fecha'].max()
casos_ccaa_last = todos_ccaa[['fecha', 'ccaa', 'casos']].copy()
casos_ccaa_last.drop(casos_ccaa_last[casos_ccaa_last.fecha != last_date].index, inplace=True)
casos_ccaa_last.drop('fecha', axis=1, inplace=True)
json_file = to_json(casos_ccaa_last, ['ccaa'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_ccaa_1_dato.json-stat')

# Datos nacionales acumulados diarios
# fecha,casos,altas,fallecimientos,ingresos_uci,hospitalizados
nacional = data[etl_cfg.input.files.nacional]
nacional = delay_date(nacional)
nacional.set_index('fecha')
nacional.rename(columns={
    'casos_total': 'casos-acumulado',
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

# Tasa de variación diaria (porcentaje)
# T(d) = 100 * ((Casos(d) - Casos(d-1))/Casos(d-1))
casos_nacional_tasa = nacional_acumulado[['fecha', 'casos']].copy()
casos_nacional_tasa.reset_index(drop=True, inplace=True)
for i in range(1, len(casos_nacional_tasa)):
    if casos_nacional_tasa.loc[i-1, 'casos'] > 0:
        casos_nacional_tasa.loc[i, 'variacion'] = 100 * (( \
            casos_nacional_tasa.loc[i, 'casos'] - casos_nacional_tasa.loc[i-1, 'casos']) / \
            casos_nacional_tasa.loc[i-1, 'casos'])
    else:
        casos_nacional_tasa.loc[i, 'variacion'] = None
casos_nacional_tasa.drop('casos', axis=1, inplace=True)
json_file = to_json(casos_nacional_tasa, ['fecha'], ['variacion'])
write_to_file(json_file, etl_cfg.output.path + 'casos_nacional_variacion.json-stat')

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

# Cifras más recientes
nacional_last = nacional.tail(1)
# altas
altas_nacional_last = nacional_last[['fecha', 'altas-acumulado']].copy()
altas_nacional_last.rename(columns={'altas-acumulado': 'altas'}, inplace=True)
json_file = to_json(altas_nacional_last, ['fecha'], ['altas'])
write_to_file(json_file, etl_cfg.output.path + 'altas_nacional_1_dato.json-stat')
# casos
casos_nacional_last = nacional_last[['fecha', 'casos-acumulado']].copy()
casos_nacional_last.rename(columns={'casos-acumulado': 'casos'}, inplace=True)
json_file = to_json(casos_nacional_last, ['fecha'], ['casos'])
write_to_file(json_file, etl_cfg.output.path + 'casos_nacional_1_dato.json-stat')
# fallecidos
fallecidos_nacional_last = nacional_last[['fecha', 'fallecidos-acumulado']].copy()
fallecidos_nacional_last.rename(columns={'fallecidos-acumulado': 'fallecidos'}, inplace=True)
json_file = to_json(fallecidos_nacional_last, ['fecha'], ['fallecidos'])
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_nacional_1_dato.json-stat')
# hospital
hospital_nacional_last = nacional_last[['fecha', 'hospital-acumulado']].copy()
hospital_nacional_last.rename(columns={'hospital-acumulado': 'hospital'}, inplace=True)
json_file = to_json(hospital_nacional_last, ['fecha'], ['hospital'])
write_to_file(json_file, etl_cfg.output.path + 'hospital_nacional_1_dato.json-stat')
# uci
uci_nacional_last = nacional_last[['fecha', 'uci-acumulado']].copy()
uci_nacional_last.rename(columns={'uci-acumulado': 'uci'}, inplace=True)
json_file = to_json(uci_nacional_last, ['fecha'], ['uci'])
write_to_file(json_file, etl_cfg.output.path + 'uci_nacional_1_dato.json-stat')

# Series diarias
#altas
altas_nacional_diario = nacional[['fecha', 'altas']]
json_file = to_json(altas_nacional_diario, ['fecha'], ['altas'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'altas_nacional_diario.json-stat')
# casos
casos_nacional_diario = nacional[['fecha', 'casos']]
json_file = to_json(casos_nacional_diario, ['fecha'], ['casos'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'casos_nacional_diario.json-stat')
# fallecidos
fallecidos_nacional_diario = nacional[['fecha', 'fallecidos']]
json_file = to_json(fallecidos_nacional_diario, ['fecha'], ['fallecidos'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_nacional_diario.json-stat')
# hospital
hospital_nacional_diario = nacional[['fecha', 'hospital']]
json_file = to_json(hospital_nacional_diario, ['fecha'], ['hospital'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'hospital_nacional_diario.json-stat')
# uci
uci_nacional_diario = nacional[['fecha', 'uci']]
json_file = to_json(uci_nacional_diario, ['fecha'], ['uci'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'uci_nacional_diario.json-stat')

# Datos nacionales por rango de edad y sexo
nacional_edad = data[etl_cfg.input.files.nacional_edad]
nacional_edad.drop(nacional_edad[nacional_edad.rango_edad == 'Total'].index, inplace=True)
nacional_edad.drop(nacional_edad[nacional_edad.sexo == 'ambos'].index, inplace=True)
last_date = nacional_edad.fecha.max()
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
casos = delay_date(casos)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_diario.json-stat')

# tasa de variación diaria (porcentaje)
# T(d) = 100 * ((Casos(d) - Casos(d-1))/Casos(d-1))
casos_tasa = casos_acumulado[['fecha', 'casos']].copy()
casos_tasa.drop(casos_tasa[casos_tasa.index < 9].index, inplace=True)
casos_tasa.reset_index(drop=True, inplace=True)
for i in range(1, len(casos_tasa)):
    if casos_tasa.loc[i-1, 'casos'] > 0:
        casos_tasa.loc[i, 'variacion'] = 100 * (( \
            casos_tasa.loc[i, 'casos'] - casos_tasa.loc[i-1, 'casos']) / \
            casos_tasa.loc[i-1, 'casos'])
    else:
        casos_tasa.loc[i, 'variacion'] = None
casos_tasa.drop('casos', axis=1, inplace=True)
json_file = to_json(casos_tasa, ['fecha'], ['variacion'])
write_to_file(json_file, etl_cfg.output.path + 'casos_cantabria_variacion.json-stat')

# Altas en Cantabria
# fecha,cod_ine,CCAA,total
altas = data[etl_cfg.input.files.altas]
altas = delay_date(altas)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'altas_cantabria_diario.json-stat')

# Ingresados en UCI en Cantabria
# fecha,cod_ine,CCAA,total
uci = data[etl_cfg.input.files.uci]
uci = delay_date(uci)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'uci_cantabria_diario.json-stat')

# Fallecidos en Cantabria
# fecha,cod_ine,CCAA,total
fallecidos = data[etl_cfg.input.files.fallecidos]
fallecidos = delay_date(fallecidos)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas acumulado'}}
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'fallecidos_cantabria_acumulado.json-stat')

# diario
fallecidos_diario = fallecidos[['fecha', 'fallecidos']].copy()
json_file = to_json(fallecidos_diario, ['fecha'], ['fallecidos'])
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = \
    {'fallecidos': {'decimals': 0, 'label': 'Número de personas'}}
json_file = json.dumps(json_obj)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = etl_cfg.metadata.todos_cantabria
json_file = json.dumps(json_obj)
write_to_file(json_file, etl_cfg.output.path + 'todos_cantabria.json-stat')

# Comparación casos Cantabria y España
espana = data[etl_cfg.input.files.nacional]
espana = delay_date(espana)
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
json_obj = json.loads(json_file)
json_obj['dimension']['Variables']['category']['unit'] = etl_cfg.metadata.casos_cantabria_espana
json_file = json.dumps(json_obj)
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

print("Proceso terminado con éxito")
