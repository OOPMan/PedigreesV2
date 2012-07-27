import logging
import os
import sys
import csv
import datetime
from contextlib import closing
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy import Column, Integer, Date, Boolean, String, create_engine, distinct, not_
from dbfpy import dbf

__author__ = 'adamj'

# ORM
Base = declarative_base()
class Animal(Base):
    __tablename__ = 'animals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sire_id = Column(Integer, nullable=True, default=None)
    dam_id = Column(Integer, nullable=True, default=None)
    birth_date = Column(Date, nullable=True, default=None)
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

def init_database(settings_file):
    logging.info('Performing Database Init')
    settings, engine, session_class = init(settings_file)
    Animal.metadata.create_all(engine)

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
    column_names_list = settings.get('column_names_list', [])
    column_value_coercion_map = settings.get('column_value_coercion_map', {})
    column_name_coercion_map = settings.get('column_name_coercion_map', {})
    reader = csv.reader(open(input_file, 'r'))
    header = reader.next()
    rows = {}
    auto_id = -1
    for row in reader:
        zipped_row = zip(header, row)
        row_id_value = auto_id
        row_dict = {}
        for column, value in zipped_row:
            if column in column_names_list and column_name_coercion_map.get(column) is not None:
                value = column_value_coercion_map.get(column, lambda v: v)(value)
                row_dict[column_name_coercion_map[column]] = value
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
        existing_animal_ids = [a.id for a in session.query(Animal.id).all()]
        for row_id in row_ids.difference(existing_animal_ids):
            session.add(Animal(**rows[row_id]))
            added +=1
        if update:
            for row_id in row_ids.intersection(existing_animal_ids):
                session.query(Animal).filter(Animal.id == row_id).update(rows[row_id])
                updated +=1
        session.commit()
        logging.info('Added %d Animals. Updated %d Animals' % (added, updated))

def fix_misgenders(settings_file):
    ''' Connects to the database and corrects the gender values for all animals '''
    logging.info('Performing Misgender Fix')
    settings, engine, session_class = init(settings_file)
    gender_map = settings.get('gender_map', {})
    with closing(session_class()) as session:
        parent = aliased(Animal)
        child = aliased(Animal)
        male_females = [a[0] for a in session.query(child.dam_id)\
                                             .outerjoin(parent, parent.id == child.dam_id)\
                                             .filter(parent.sex == gender_map['MALE'])\
                                             .group_by(child.dam_id)\
                                             .all()]
        female_males = [a[0] for a in session.query(child.sire_id)\
                                             .outerjoin(parent, parent.id == child.sire_id)\
                                             .filter(parent.sex == gender_map['FEMALE'])\
                                             .group_by(child.sire_id)\
                                             .all()]
        logging.info('Detected misassigned %d Males and misassigned %d Females' % (len(male_females), len(female_males)))
        logging.info('Male Females: %s' % ','.join([str(aid) for aid in male_females]))
        logging.info('Female Males: %s' % ','.join([str(aid) for aid in female_males]))
        for animal_id in male_females:
            session.query(Animal).filter(Animal.id == animal_id).update({'sex': gender_map['FEMALE']})
        for animal_id in female_males:
            session.query(Animal).filter(Animal.id == animal_id).update({'sex': gender_map['MALE']})
        session.commit()

def fix_invalid_genders(settings_file):
    logging.info('Performing Invalid Gender Fix')
    settings, engine, session_class = init(settings_file)
    gender_map = settings.get('gender_map', {})
    with closing(session_class()) as session:
        gender, gender_value = gender_map.items()[0]
        invalid_genders = session.query(Animal).filter(not_(Animal.sex.in_(gender_map.values())))
        logging.info('Detected %d Animals with Invalid Gender Values. Resetting to %s' % (invalid_genders.count(), gender))
        invalid_genders.update({'sex': gender_value}, synchronize_session='fetch')
        session.commit()

def fix_birth_dates(settings_file):
    logging.info('Performing Birth Date Fix')
    settings, engine, session_class = init(settings_file)
    with closing(session_class()) as session:
        birth_dates = session.query(Animal.id).filter(Animal.birth_date == None)
        logging.info('Detected %d NULL birth dates' % birth_dates.count())
        counter = 0
        for animal_id in [animal.id for animal in birth_dates]:
            animal_id_str = str(animal_id)
            year = int(animal_id_str[:3]) if animal_id_str[0] == '1' else int(animal_id_str[:2])
            if year >= 79:
                session.query(Animal).filter(Animal.id == animal_id).update({'birth_date': datetime.date(year + 1900, 1, 1)})
                counter += 1
        session.commit()
        logging.info('Corrected %d NULL birth dates' % counter)
        if birth_dates.count() > 0:
            logging.info('Unable to correct birth dates for the following animals: %s' % ','.join([str(a[0]) for a in birth_dates]))

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
        for sire_id in [a[0] for a in session.query(distinct(child.sire_id))\
                             .outerjoin(parent, child.sire_id == parent.id)\
                             .filter((child.sire_id != None) & (parent.id == None))]:
            session.add(Animal(id = sire_id, sex=gender_map['MALE'], birth_date=datetime.date(1900+int(str(sire_id)[:2]),1,1), dummy_animal=True))
            sires += 1
        for dam_id in [a[0] for a in session.query(distinct(child.dam_id))\
                             .outerjoin(parent, child.dam_id == parent.id)\
                             .filter((child.dam_id != None) & (parent.id == None))]:
            session.add(Animal(id = dam_id, sex=gender_map['FEMALE'], birth_date=datetime.date(1900+int(str(dam_id)[:2]),1,1), dummy_animal=True))
            dams +=1
        session.commit()
        logging.info('Added %d Dummy Sires and %s Dummy Dams' % (sires, dams))

def set_base_population_members(settings_file, method='standard'):
    ''' Connects to the database and updates the base_population_member for all animals based on algorithm'''
    logging.info('Performing Base Population Marking using %s Method' % method.capitalize())
    settings, engine, session_class = init(settings_file)
    with closing(session_class()) as session:
        if method == 'standard':
            base_members_query = session.query(Animal).filter( (Animal.base_population_member == False) & (Animal.group.in_([0,1]) & (Animal.birth_date < datetime.date(2003,1,1)) ) )
            logging.info('Detected %d Base Population Members (Animals with Group value equal to either 0 or 1 born before 01/01/2003)' % base_members_query.count())
            base_members_query.update({'sire_id': None, 'dam_id': None, 'base_population_member': True}, synchronize_session='fetch')
        elif method == 'noparents':
            sire = aliased(Animal)
            dam = aliased(Animal)
            child = aliased(Animal)
            base_member_ids = [a[0] for a in session.query(distinct(child.id))\
                                                  .outerjoin(sire, child.sire_id == sire.id)\
                                                  .outerjoin(dam, child.dam_id == dam.id)\
                                                  .filter((Animal.base_population_member == False) & (sire.id == None) & (dam.id == None))]
            logging.info('Detected %d possible Base Population Members' % len(base_member_ids))
            for base_member_id in base_member_ids:
                session.query(Animal)\
                       .filter(Animal.id == base_member_id)\
                       .update({'sire_id': None, 'dam_id': None, 'base_population_member': True, 'notes': "Added as Base Population Member due to Parents Not Existing in Database"})
        session.commit()

def locate_disconnected_animals(settings_file, input_file=None, delete=False):
    ''' Connects to the database and locates all animals that are disconnected from the dataset '''
    logging.info('Performing Disconnected Animal Detection')
    if input_file:
        logging.info('Will filter Animal IDs using data from %s' % input_file)
    if delete:
        logging.info('Will delete located Animals')
    settings, engine, session_class = init(settings_file)
    with closing(session_class()) as session:
        sire = aliased(Animal)
        dam = aliased(Animal)
        child1 = aliased(Animal)
        child2 = aliased(Animal)
        disconnected_animals = session.query(Animal)\
                                      .outerjoin(sire, Animal.sire_id == sire.id)\
                                      .outerjoin(dam, Animal.dam_id == dam.id)\
                                      .outerjoin(child1, Animal.id == child1.sire_id)\
                                      .outerjoin(child2, Animal.id == child2.dam_id)\
                                      .filter( (sire.id == None) & (dam.id == None) & (child1.id == None) & (child2.id == None) )
        if input_file:
            disconnected_animals = disconnected_animals.filter(Animal.id.in_(load_csv(settings, input_file).keys()))
        logging.info('Detected %d total disconnected Animals' % disconnected_animals.count())
        logging.info('Disconnected Animal IDs: %s' % ','.join([str(animal.id) for animal in disconnected_animals]))
        if delete:
            logging.info('Deleting disconnected Animals')
            for animal in disconnected_animals:
                session.delete(animal)
        session.commit()

def get_rows_for_generate(session_class, groups = None):
    with closing(session_class()) as session:
        query = session.query(Animal)
        # Allow for Group Tuning
        if isinstance(groups, list):
            query = query.filter( (Animal.group.in_(groups)) | (Animal.base_population_member == True) )
        row_sets = [ [animal for animal in query] ]
        while True:
            animals = row_sets[0]
            animal_ids = [animal.id for animal in animals]
            parent_ids = set([animal.sire_id for animal in animals] + [animal.dam_id for animal in animals])
            missing_parent_ids = parent_ids.difference(animal_ids)
            if not missing_parent_ids:
                break
            row_sets.insert(0, [animal for animal in session.query(Animal).filter(Animal.id.in_(missing_parent_ids))])
        return { animal.id: animal for row_set in row_sets for animal in row_set }.values()

def generate_popreport_input(settings_file, output_file, groups=None):
    ''' Connects to the database and dumps the data into a file formatted for PopReport '''
    logging.info('Generating PopReport Input File')
    settings, engine, session_class = init(settings_file)
    with open(output_file, 'w') as output:
        lines = []
        for animal in get_rows_for_generate(session_class, groups):
            lines.append('|'.join([str(c) for c in [animal.id if animal.id is not None else '',
                                                    animal.sire_id if animal.sire_id is not None else '',
                                                    animal.dam_id if animal.dam_id is not None else '',
                                                    animal.birth_date.strftime('%Y-%m-%d') if animal.birth_date is not None else '',
                                                    animal.sex if animal.sex is not None else '']]))
        output.write('\n'.join(lines))
        logging.info('Wrote %d Animals to %s' % (len(lines), output_file))

def generate_endog_input(settings_file, output_file, groups=None):
    ''' Connects to the database and dumps the data into a file formatted for EndDog '''
    logging.info('Generating Endog Input File')
    settings, engine, session_class = init(settings_file)
    with closing(dbf.Dbf(output_file, new=True)) as db:
        db.addField(
            ('ID', 'N', 16, 0),
            ('SIRE_ID', 'N', 16, 0),
            ('DAM_ID', 'N', 16, 0),
            ('BIRTH_DATE', 'D'),
            ('S', 'N', 1, 0),
            ('GROUP','N', 16, 0),
            ('REFERENCE', 'N', 1, 0)
            )
        animals = get_rows_for_generate(session_class, groups)
        for animal in animals:
            record = db.newRecord()
            record['ID'] = animal.id if animal.id else 0
            record['SIRE_ID'] = animal.sire_id if animal.sire_id else 0
            record['DAM_ID'] = animal.dam_id if animal.dam_id else 0
            record['BIRTH_DATE'] = animal.birth_date
            record['S'] = animal.sex
            record['GROUP'] = animal.group
            record['REFERENCE'] = int(animal.base_population_member)
            record.store()
        logging.info('Wrote %d Animals to %s' % (len(animals), output_file))
