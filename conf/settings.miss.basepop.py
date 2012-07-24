from functions import coerce_date_value, coerce_animal_id, coerce_int

__author__ = 'adamj'

id_column_name = 'NOMMER'

column_names_list = [id_column_name, 'VAAR', 'MOER', 'GR','YR', 'SEX','BASE']

column_value_coercion_map = {
    id_column_name: coerce_animal_id,
    'VAAR': coerce_animal_id,
    'MOER': coerce_animal_id,
    'GR': coerce_int,
    'SEX': coerce_int,
    'YR': coerce_date_value,
    'BASE': lambda v: True,
}

column_name_coercion_map = {
    id_column_name: 'id',
    'VAAR': 'sire_id',
    'MOER': 'dam_id',
    'GR': 'group',
    'YR': 'birth_date',
    'SEX': 'sex',
    'BASE': 'base_population_member',
}
