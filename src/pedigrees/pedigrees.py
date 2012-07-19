import logging
import os
import sys
from contextlib import closing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker
from sqlalchemy import Column, Integer, Date, ForeignKey, Boolean, Float, create_engine
import csv

__author__ = 'adamj'

# ORM
Base = declarative_base()
class Animal(Base):
    __tablename__ = 'animals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sire_id = Column(Integer, nullable=True)
    dam_id = Column(Integer, nullable=True)
    birth_date = Column(Date, nullable=False)
    group = Column(Integer, nullable=True, default=-1)
    sex = Column(Integer, nullable=True, default=MALE)
    base_population_member = Column(Boolean, nullable=False, default=False)
    dummy_animal = Column(Boolean, nullable=False, default=False)

def load_settings(settings_file):
    ''' Loads the given Settings Python script, returning a dict containing values '''
    settings_file = os.path.abspath(settings_file)
    sys.path.append(os.path.dirname(settings_file))
    settings = {}
    logging.info('Loading settings from %s' % settings_file)
    execfile(settings_file, settings)
    return settings

def load_csv(settings, input_file):
    ''' Opens and input CSV and parses the contents a row dict '''
    input_file = os.path.abspath(input_file)
    logging.info('Loading CSV data from %s' % input_file)
    column_coercion_map = settings.get('column_coercion_map', {})
    reader = csv.reader(input_file)
    header = reader.next()
    rows = {}
    auto_id = -1
    for row in reader:
        zipped_row = zip(header, row)
        row_id_value = auto_id
        row_dict = {}
        for column, value in zipped_row:
            if column in settings.get('column_names_list', []):
                value = column_coercion_map.get(column, lambda v: v)(value)
                row_dict[column] = value
                if column == settings.get('id_column_name'):
                    row_id_value = value
        rows[row_id_value] = row_dict
        if row_id_value == auto_id:
            auto_id -= 1
    return rows

def import_csv(settings_file, input_file, update=True):
    ''' Opens an input CSV and parses the contents into Records
    '''
    settings = load_settings(settings_file)
    engine = create_engine(settings['connection_string'])
    session_class = sessionmaker(bind=engine)
    rows = load_csv(settings, input_file)
    row_ids = set(rows.keys())
    with closing(session_class()) as session:
        existing_animal_ids = session.query(Animal.id).all()
        for row_id in row_ids.difference(existing_animal_ids):
            session.add(Animal(**rows[row_id]))
        if update:
            for row_id in row_ids.intersection(existing_animal_ids):
                session.query(Animal).filter(Animal.id == row_id).update(**rows[row_id])
        session.commit()

