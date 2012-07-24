from settings import connection_string, gender_map
from functions import coerce_date_value, coerce_animal_id, coerce_int

__author__ = 'adamj'

id_column_name = 'ID'

column_names_list = [id_column_name, 'SIREID', 'DAMID', 'YR','GR', 'SEX']

column_value_coercion_map = {
    id_column_name: coerce_animal_id,
    'SIREID': coerce_animal_id,
    'DAMID': coerce_animal_id,
    'GR': coerce_int,
    'SEX': coerce_int,
    'YR': coerce_date_value,
}

column_name_coercion_map = {
    id_column_name: 'id',
    'SIREID': 'sire_id',
    'DAMID': 'dam_id',
    'GR': 'group',
    'YR': 'birth_date',
    'SEX': 'sex',
}
