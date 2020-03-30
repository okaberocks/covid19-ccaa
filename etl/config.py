from beautifuldict.baseconfig import Baseconfig

from decouple import config

from pkg_resources import resource_filename


params = {
    'input': {
        'source': config('SOURCE'),
        'dir_path': config('SOURCE') + 'COVID 19/',
        'files': {
            'altas': 'ccaa_covid19_altas_long.csv',
            'casos': 'ccaa_covid19_casos_long.csv',
            'fallecidos': 'ccaa_covid19_fallecidos_long.csv',
            'hospital': 'ccaa_covid19_hospitalizados_long.csv',
            'nacional': 'nacional_covid19.csv',
            'nacional_edad': 'nacional_covid19_rango_edad.csv',
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
