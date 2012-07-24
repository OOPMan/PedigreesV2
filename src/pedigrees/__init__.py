import argparse
import os
import pedigrees
import logging

__author__ = 'adamj'

if __name__ == '__main__':
    # Init Logging
    logging.basicConfig(level=logging.INFO)
    # Set up argument parser
    parser = argparse.ArgumentParser()
    mode_choices = ['init_database', 'import_csv', 'fix_misgenders','fix_birth_dates', 'generate_dummy_animals', 'set_base_population_members', 'generate_popreport_input', 'generate_endog_input']
    parser.add_argument('mode',
                        type=lambda s: s.lower(),
                        choices=mode_choices,
                        help='An operation mode. One of: %s' % ', '.join(mode_choices))
    parser.add_argument('settings_file', help='A python file containing settings data that will be imported for use by the application')
    parser.add_argument('-f', '--file', type=os.path.abspath, help='A file to be used for data input/output')
    parser.add_argument('-g', '--groups', nargs='+', type=lambda v: int(v), help='A list of Group values to include in processing. Only applies to generate_?_input modes')
    method_choices = ['standard', 'noparents']
    default_method_choice = method_choices[0]
    parser.add_argument('-m', '--method', type=lambda s: s.lower(), default=default_method_choice,
                        help='Select method used for set_base_population_members. One of: %s. Only applies to set_base_population_members mode. Defaults to %s' % (', '.join(method_choices), default_method_choice))
    parser.add_argument('-s', '--skip-update', dest='update', action='store_false', help='Also Update existing animals based on input data. Only applies to import_csv mode')
    #TODO: Add logging related args
    # Process arguments and prepare
    namespace = parser.parse_args()
    args = [namespace.settings_file]
    if namespace.mode in ['import_csv', 'generate_popreport_input', 'generate_endog_input'] and namespace.file:
        args.append(namespace.file)
    if namespace.mode == 'import_csv':
        args.append(namespace.update)
    elif namespace.mode == 'set_base_population_members':
        args.append(namespace.method)
    elif namespace.mode in ['generate_popreport_input', 'generate_endog_input']:
        args.append(namespace.groups)
    #TODO: Process logging related args
    # Execute
    getattr(pedigrees, namespace.mode)(*args)
