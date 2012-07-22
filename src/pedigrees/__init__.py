import argparse
import os
import pedigrees

__author__ = 'adamj'

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('mode',
                        type=lambda s: s.lower(),
                        choices=['import_csv', 'fix_genders', 'generate_dummy_animals', 'generate_popreport_input', 'generate_endog_input'],
                        help='An operation mode. One of: import_csv, fix_genders, generate_dummy_animals, generate_popreport_input or generate_endog_input')
    parser.add_argument('settings_file', help='A python file containing settings data that will be imported for use by the application')
    parser.add_argument('-f', '--file', type=os.path.abspath, help='A file to be used for data input/output')
    parser.add_argument('-g', '--groups', nargs='+', type=lambda v: [int(s) for s in v], help='A list of Group values to include in processing. Only applies to generate_?_input modes')
    parser.add_argument('-a', '--alternate', action='store_true', help='Also use alternate methods of calculation. Only applies to set_base_population_members mode')
    parser.add_argument('-s', '--skip-update', dest='update', action='store_false', help='Also Update existing animals based on input data. Only applies to import_csv mode')
    # Process arguments and prepare
    namespace, errors = parser.parse_args()
    args = [namespace.settings_file]
    if namespace.mode in ['import_csv', 'generate_popreport_input', 'generate_endog_input'] and namespace.file:
        args.append(namespace.file)
    if namespace.mode == 'import_csv':
        args.append(namespace.update)
    elif namespace.mode == 'set_base_population_members':
        args.append(namespace.alternate)
    elif namespace.mode in ['generate_popreport_input', 'generate_endog_input']:
        args.append(namespace.groups)
    # Execute
    getattr(pedigrees, namespace.mode)(*args)
