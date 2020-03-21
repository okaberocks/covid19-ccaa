from beautifuldict.baseconfig import Baseconfig

from decouple import config

from pkg_resources import resource_filename

params = {
    'input': {
        'source': config('SOURCE'),
        'dir_path': config('SOURCE') + 'COVID 19/',
        'files': {
            'nacional': 'nacional_covid19.csv',
            'casos': 'ccaa_covid19_casos.csv',
            'altas': 'ccaa_covid19_altas.csv',
            'uci': 'ccaa_covid19_uci.csv',
            'fallecidos': 'ccaa_covid19_fallecidos.csv'
        }
    },
    'output': {
        'path': resource_filename(__name__, 'data/')
    }
}
etl_cfg = Baseconfig(params)
