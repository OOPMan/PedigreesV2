import logging
import os
import sys
import csv
import dbfpy
import datetime
from contextlib import closing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import Column, Integer, Date, Boolean, String, create_engine

__author__ = 'adamj'

# ORM
Base = declarative_base()
class Animal(Base):
    __tablename__ = 'animals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sire_id = Column(Integer, nullable=True, default=None)
    dam_id = Column(Integer, nullable=True, default=None)
    birth_date = Column(Date, nullable=False)
    group = Column(Integer, nullable=True, default=-1)
    sex = Column(Integer, nullable=True, default=-1)
    base_population_member = Column(Boolean, nullable=False, default=False)
    dummy_animal = Column(Boolean, nullable=False, default=False)
    notes = Column(String, nullable=True, default=None)

def init(settings_file):
    ''' Performs common init '''
    settings = load_settings(settings_file)
    engine = create_engine(settings['connection_string'])
    session_class = sessionmaker(bind=engine)
    return (settings, engine, session_class)

def load_settings(settings_file):
    ''' Loads the given Settings Python script, returning a dict containing values '''
    sys.path.append(os.path.dirname(settings_file))
    settings = {}
    logging.info('Loading settings from %s' % settings_file)
    execfile(settings_file, settings)
    return settings

def load_csv(settings, input_file):
    ''' Opens and input CSV and parses the contents a row dict '''
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
    ''' Opens an input CSV and parses the contents into Records '''
    logging.info('Performing CSV Import')
    settings, engine, session_class = init(settings_file)
    rows = load_csv(settings, input_file)
    row_ids = set(rows.keys())
    added = 0
    updated = 0
    with closing(session_class()) as session:
        existing_animal_ids = session.query(Animal.id).all()
        for row_id in row_ids.difference(existing_animal_ids):
            session.add(Animal(**rows[row_id]))
            added +=1
        if update:
            for row_id in row_ids.intersection(existing_animal_ids):
                session.query(Animal).filter(Animal.id == row_id).update(**rows[row_id])
                updated +=1
        session.commit()
        logging.info('Added %d Animals. Updated %d Animals' % (added, updated))

def fix_genders(settings_file):
    ''' Connects to the database and corrects the gender values for all animals '''
    logging.info('Performing Gender Fix')
    settings, engine, session_class = init(settings_file)
    gender_map = settings.get('gender_map', {})
    with closing(session_class()) as session:
        parent = aliased(Animal)
        child = aliased(Animal)
        male_females = session.query(parent)\
                              .outerjoin(child, (child.dam_id == parent.id) & (parent.sex != gender_map['FEMALE']) & (child.id != None) )\
                              .group_by(parent.id)
        female_males = session.query(parent)\
                              .outerjoin(child, (child.sire_id == parent.id) & (parent.sex != gender_map['MALE']) & (child.id != None))\
                              .group_by(parent.id)
        logging.info('Detected misassigned %d Males and misassigned %d Females' % (male_females.count(), female_males.count()))
        male_females.update(sex = gender_map['FEMALE'])
        female_males.update(sex = gender_map['MALE'])
        session.commit()

def generate_dummy_animals(settings_file):
    ''' Connects to the database and generates dummy parents for all animals whose parents do not exist '''
    logging.info('Performing Dummy Animal Generation')
    settings, engine, session_class = init(settings_file)
    gender_map = settings.get('gender_map', {})
    with closing(session_class()) as session:
        parent = aliased(Animal)
        child = aliased(Animal)
        sires = 0
        dams = 0
        for animal in session.query(child)\
                             .outerjoin(parent, (child.sire_id != None) & (child.sire_id == parent.id) & (parent.id == None))\
                             .group_by(child.id):
            session.add(Animal(id = animal.sire_id, sex=gender_map['MALE'], birth_date=datetime.date(1900+int(str(animal.sire_id)[:2]),1,1), dummy_animal=True))
            sires += 1
        for animal in session.query(child)\
                             .outerjoin(parent, (child.dam_id != None) & (child.dam_id == parent.id) & (parent.id == None))\
                             .group_by(child.id):
            session.add(Animal(id = animal.dam_id, sex=gender_map['FEMALE'], birth_date=datetime.date(1900+int(str(animal.sire_id)[:2]),1,1), dummy_animal=True))
            dams +=1
        session.commit()
        logging.info('Added %d Dummy Sires and %s Dummy Dams' % (sires, dams))

def set_base_population_members(settings_file, alternate=False):
    ''' Connects to the database and updates the base_population_member for all animals based on algorithm'''
    logging.info('Performing Base Population Marking')
    settings, engine, session_class = init(settings_file)
    with closing(session_class()) as session:
        base_members_query = session.query(Animal).filter(Animal.group.in_([0,1]))
        logging.info('Detected %d Base Population Members (Animals with Group value equal to either 0 or 1)' % base_members_query.count())
        base_members_query.update(base_population_member=True)
        if alternate:
            parent = aliased(Animal)
            child = aliased(Animal)
            alternate_base_members_query = session.query(child)\
                                                  .outerjoin(parent, ((child.sire_id == parent.id) | (child.dam_id == parent.id)) & (parent.id == None))\
                                                  .group_by(child.id)
            logging.info('Detected %d possible Base Population Members' % alternate_base_members_query.count())
            alternate_base_members_query.update(base_population_member=True, notes="Added as Base Population Member due to Parents Not Existing in Database")
        session.commit()

def generate_popreport_input(settings_file, output_file, groups=None):
    ''' Connects to the database and dumps the data into a file formatted for PopReport '''
    logging.info('Generating PopReport Input File')
    settings, engine, session_class = init(settings_file)
    with closing(session_class()) as session, open(output_file, 'w') as output:
        lines = []
        query = session.query(Animal)
        # Allow for Group Tuning
        if isinstance(groups, list):
            query = query.filter(Animal.group.in_(groups))
        for animal in query:
            lines.append('|'.join([str(c) for c in [animal.id, animal.sire_id, animal.dam_id, animal.birth_date.strftime('%Y-%m-%d'), animal.sex]]))
        output.write('\n'.join(lines))
        logging.info('Wrote %d Animals to %s' % (len(lines), output_file))

def generate_endog_input(settings_file, output_file, groups=None):
    ''' Connects to the database and dumps the data into a file formatted for EndDog '''
    #TODO


