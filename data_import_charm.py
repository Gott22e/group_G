#!/usr/bin/env python3
"""
TODO
"""
# Import packages
import sshtunnel
import getpass
import pandas as pd
import pymysql
import numpy as np
import sqlalchemy

# TODO deal with NaN, None, Null, etc.

# Global variables:
Create_new_table = True
In_jyptr = False  # TODO fix
Tunnel = None
In_pycharm = True  # TODO fix

# TODO temp_table = pd.read_csv("short_test.csv")
# TODO  print(temp_table)


class ImportTools:
    def __init__(self):
        # TODO
        # Variables we will add in:
        self.our_added_vars = ['study_name', 'study_year', 'geo_cord_system', 'utm_cord_system']
        self.table_name = "cr"
        pass

    @staticmethod
    def read_in_csv(filename):
        temp_table = pd.read_csv(filename, sep="|", header=1)
        return temp_table

    @staticmethod
    def execute_query(query):
        # TODO
        # TODO CHange to login!!
        # create the connection to the mysql database
        if In_jyptr:
            connection = pymysql.connect(db='group_G', user='anau',
                                         passwd=getpass.getpass(prompt='Password (bioed): ', stream=None),
                                         port=Tunnel.local_bind_port)
        else:  # TODO test
            connection = pymysql.connect(user="test", password="test", db="group_G", port=4253)
        # execute the query and fetch the results go get the genes
        with connection.cursor() as cursor:
            # execute
            cursor.execute(query)
            # retrieve your results
            temp = cursor.fetchall()
            for row in temp:
                print(temp)
            cursor.close()
        connection.close()

    def create_statement(self):
        """
        Makes create table statement string for master table.
        :return: string with create table statement
        """
        # Initialize create table string:
        create_string = f"CREATE TABLE {self.table_name} ( \n"
        create_string += "anid int NOT NULL AUTO_INCREMENT, \n"
        create_string += "study_name VARCHAR(200), \n"
        create_string += "study_year int, \n"
        create_string += "geo_cord_system VARCHAR(100), \n"
        create_string += "utm_cord_system VARCHAR(100), \n"
        # Variables to add (full_list controls order of columns in database):
        full_list = ['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg',
                     'anal_type', 'labsample', 'study_id', 'sample_no', 'sampcoll_id',
                     'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
                     'composite_type', 'taxon', 'sample_material', 'sample_description',
                     'subsamp_type', 'upper_depth', 'lower_depth', 'depth_units',
                     'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
                     'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units',
                     'sig_figs', 'lab_flags', 'qa_level', 'lab_conc_qual', 'validator_flags',
                     'detection_limit', 'reporting_limit', 'undetected', 'estimated',
                     'rejected', 'greater_than', 'tic', 'reportable', 'alias_id', 'comments',
                     'river_mile', 'x_coord', 'y_coord', 'utm_x', 'utm_y', 'srid',
                     'lat_WGS84_auto_calculated_only_for_mapping', 'lon_WGS84_auto_calculated_only_for_mapping']
        with open("column_names.txt", 'w') as my_file:
            for temp in self.our_added_vars:
                my_file.write(f"{temp}\n")
            for temp in full_list:
                my_file.write(f"{temp}\n")
        # Variable types:
        int_variables = ['lab_rep', "cas_rn", 'sig_figs', 'detection_limit', 'reporting_limit']
        decimal_variables = ['upper_depth', 'lower_depth', 'analyte', 'full_name', 'original_lab_result', 'meas_value',
                             'river_mile', 'x_coord', 'y_coord', 'srid',
                             'utm_x', 'utm_y', 'lat_WGS84_auto_calculated_only_for_mapping',
                             'lon_WGS84_auto_calculated_only_for_mapping']
        date_variables = ['sample_date']
        # TODO: is lab_conc_qual really string?
        # TODO: booleans currently as strings: 'undetected', 'estimated',
        #        'rejected', 'greater_than', 'tic', 'reportable',
        string_variables = ['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg', 'anal_type', 'labsample',
                            'study_id', 'sample_no', 'sampcoll_id', 'sum_sample_id', 'sample_id', "study_element",
                            'composite_type', 'taxon', 'sample_material', 'sample_description', 'subsamp_type',
                            'depth_units', 'material_analyzed', 'method_code', 'meas_basis', 'units', 'lab_flags',
                            'qa_level', 'lab_conc_qual', 'validator_flags',
                            'undetected', 'estimated',
                            'rejected', 'greater_than', 'tic', 'reportable',
                            'alias_id']
        string_variables_long = ['comments']
        # Loop for variables to add (done this way to preserve order)
        for temp in full_list:
            create_string += f"{temp} "
            if temp in string_variables:
                create_string += "VARCHAR(200)"
            elif temp in int_variables:
                create_string += "INT"
            elif temp in decimal_variables:
                create_string += "DECIMAL"
            elif temp in date_variables:
                create_string += f"DATETIME"
            elif temp in string_variables_long:
                # TODO is this really best way to do this for comments?
                create_string += "VARCHAR(2000)"
            else:
                print(f"Error: variable missing from data type lists: {temp}")
            create_string += ", \n"
        # Finish statement:
        create_string += "PRIMARY KEY (anid) \n"
        create_string += ") ENGINE = INNODB;"
        # Return create table string:
        return create_string

    def create_table(self):
        # TODO
        drop_existing = f"DROP TABLE IF EXISTS {self.table_name};"
        create_statement = self.create_statement()
        if not In_pycharm:
            ImportTools.execute_query(drop_existing)
            ImportTools.execute_query(create_statement)
        print(create_statement)

    @staticmethod
    def compare_column_names(columns, ref="column_names.txt"):
        # TODO
        # To store reference columns:
        ref_cols = []
        # Columns in new data frame:
        cols = set(columns)
        # Grab saved reference columns:
        with open(ref, 'r') as my_file:
            for line in my_file:
                line = line.strip("\n")
                ref_cols.append(line)
        # Reference, shared, new, and missing columns:
        ref_cols = set(ref_cols)
        shared_cols = ref_cols.intersection(cols)
        new_cols = cols.difference(ref_cols)
        miss_cols = ref_cols.difference(cols)
        return list(shared_cols), list(new_cols), list(miss_cols)

    @staticmethod
    def clean_col_names():
        # TODO
        pass

class KnownStudyTemplates:
    def __init__(self):
        # TODO
        # Dictionary of study templates:
        templates = {}
        # Dictionary of studies and what template to use:
        study_temps = {}

    def mod_temps(self):
        # TODO
        self.save_temps()
        pass

    def save_temps(self):
        # TODO
        pass

    def load_temps(self):
        # TODO
        pass


class ImportStudy(ImportTools):
    def __init__(self, the_file):
        # TODO
        super().__init__()
        self.the_file = the_file
        self.table = self.read_in_csv(filename=self.the_file)
        self.col_names = []
        self.shared_cols = []
        self.new_cols = []
        self.miss_cols = []
        self.insert_statement = ""

    def run_import(self):
        # TODO
        self.check_columns()
        # If there are not missing columns, do stuff:
        if not self.miss_cols:
            print("There are new columns, table columns must be modified before proceeding!!!!!")
        else:
            self.make_insert_statement()
        # TODO: do something based on new columns
        # TODO: modify to raise error?
        pass

    def check_columns(self):
        # TODO: add columns for study name & coordinate system
        self.col_names = list(self.table.columns)
        print("COLUMN NAMES:")
        print(self.col_names)
        self.shared_cols, self.new_cols, self.miss_cols = self.compare_column_names(self.col_names)
        print("Columns shared with the reference:")
        print(self.shared_cols)
        print("Columns that are new:")
        print(self.new_cols)
        print("Columns that are missing:")
        print(self.miss_cols)

    def make_insert_statement(self):
        # TODO
        insert_string = self.insert_header()
        insert_string += " \nValues "
        print(insert_string)
        temp_table = self.table.copy(deep=True)
        temp_table = temp_table.fillna("Null")
        temp_table = temp_table.values.tolist()
        #TODO remove for index, row in temp_table.iterrows():
        
            insert_string += "("
            #TODO remove values = row.values.tolist()
            for temp in values:
                insert_string += f"{temp}, "
            insert_string = insert_string[-2]
            insert_string += ")\n"
        insert_string += ";"  # TODO: does floating semicolon work?
        print(insert_string)

    def insert_header(self):
        # TODO
        build_str = f"INSERT INTO {self.table_name} ("
        for temp in self.our_added_vars:
            build_str += f"{temp}, "
        for temp in self.col_names:
            build_str += f"{temp}, "
        # Remove last two values in string:
        build_str = build_str[:-2]
        build_str += ")"
        return build_str


# main function:
def main():
    # Set up sshtunnel (for juyptr notebook):
    our_import = ImportTools()
    if In_jyptr:
        # Grab global variable:
        global Tunnel
        # connect to bioed via an ssh tunnel
        # do NOT include your password, use getpass
        Tunnel = sshtunnel.SSHTunnelForwarder(
            ('bioed.bu.edu', 22),
            ssh_username='anau',
            ssh_password=getpass.getpass(prompt='Password (bu): ', stream=None),
            remote_bind_address=('localhost', 4253))
        # the password requested here is your kerberos password that you use to access bioed
        # "activate" the ssh tunnel
        Tunnel.start()
    # Create table:
    if Create_new_table:
        our_import.create_table()
    if In_jyptr:
        Tunnel.stop()
    study1 = ImportStudy(the_file="Phase 1 Sediment.csv")
    study1.run_import()


if __name__ == '__main__':
    main()

# TODO: class to pickle data templates

# TODO: class to create initial master SQL table
# TODO: use a modfied version of creating temporary SQL tables, but with certain columns forced?

# TODO: class to create temporary SQL tables


# TODO: class to check if data template already exists, and import


# TODO: class to import novel data template & save
# TODO: Function to add new column to already created sql table?
