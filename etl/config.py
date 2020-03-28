from beautifuldict.baseconfig import Baseconfig

from decouple import config

from pkg_resources import resource_filename


params = {
    'input': {
        'source': config('SOURCE'),
        'dir_path': config('SOURCE') + 'COVID 19/',
        'files': {
            'altas': 'ccaa_covid19_altas_long.csv',
            'camas_uci': 'ccaa_camas_uci_2017.csv',
            'casos': 'ccaa_covid19_casos_long.csv',
            'fallecidos': 'ccaa_covid19_fallecidos_long.csv',
            'hospitalizados': 'ccaa_covid19_hospitalizados_long.csv',
            'mascarillas': 'ccaa_covid19_mascarillas.csv',
            'nacional': 'nacional_covid19.csv',
            'uci': 'ccaa_covid19_uci_long.csv'
        }
    },
    'output': {
        'path': resource_filename(__name__, 'data/'),
        'repository': config('REPOSITORY')
    },
    'metadata': {
        'source': 'Ministerio de Sanidad, Consumo y Bienestar Social. A partir de ficheros de datos elaborados por DATADISTA.COM',
    }
}
etl_cfg = Baseconfig(params)
