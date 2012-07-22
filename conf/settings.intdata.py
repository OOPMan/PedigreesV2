from datetime import date
from time import strptime
from settings import connection_string, gender_map

__author__ = 'adamj'

id_column_name = 'NOMMER'

column_names_list = [id_column_name, 'VAAR', 'MOER', 'GR','YR', 'SEX', 'BDAT', 'WDAT']

def coerce_date_value(value):
    return date(*strptime(value, '%Y/%m/%d')[:3])

column_coercion_map = {
    id_column_name: int,
    'VAAR': int,
    'MOER': int,
    'GR': int,
    'YR': lambda v: int(v)+1900,
    'SEX': int,
    'BDAT': coerce_date_value,
    'WDAT': coerce_date_value
}
