from settings import connection_string, gender_map
from functions import coerce_date_value, coerce_animal_id

__author__ = 'adamj'

id_column_name = 'NOMMER'

column_names_list = [id_column_name, 'VAAR', 'MOER', 'GR','YR', 'SEX', 'BDAT', 'WDAT']

column_value_coercion_map = {
    id_column_name: coerce_animal_id,
    'VAAR': coerce_animal_id,
    'MOER': coerce_animal_id,
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
