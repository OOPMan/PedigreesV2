from datetime import date
from time import strptime
from settings import connection_string, gender_map

__author__ = 'adamj'

id_column_name = 'NOMMER'

column_names_list = [id_column_name, 'VAAR', 'MOER', 'GR','YR', 'SEX', 'BDAT', 'WDAT']

def coerce_date_value(value):
    try:
        return date(*strptime(value, '%Y/%m/%d')[:3])
    except ValueError:
        return date(*strptime(value, '%m/%d/%Y')[:3])

column_value_coercion_map = {
    id_column_name: int,
    'VAAR': int,
    'MOER': int,
    'GR': int,
    'SEX': int,
    'BDAT': coerce_date_value,
}

column_name_coercion_map = {
    id_column_name: 'id',
    'VAAR': 'sire_id',
    'MOER': 'dam_id',
    'GR': 'group',
    'YR': None,
    'SEX': 'sex',
    'BDAT': 'birth_date',
    'WDAT': None,
}
