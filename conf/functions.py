from datetime import date
from time import strptime

__author__ = 'adamj'

def coerce_date_value(value):
    try:
        return date(*strptime(value, '%Y/%m/%d')[:3])
    except ValueError:
        try:
            return date(*strptime(value, '%m/%d/%Y')[:3])
        except ValueError:
            try:
                value = int(value)
                return date(1900+value,1,1)
            except ValueError:
                return None

def coerce_animal_id(value):
    value = int(value)
    return None if value == 0 else value
