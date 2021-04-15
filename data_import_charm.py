#!/usr/bin/env python3
"""
Program contains classes and functions to create data table and insert rows for the
Upper Columbia River Site database.

Program is designed to be run from within the juyptr notebook "data_import.ipynb".
(This is required for password management and tunneling.)

Updated: 2021/04/15
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
In_jyptr = True  # TODO fix
Tunnel = None
# In_pycharm used to suppress functionality that is not currently enabled:
In_pycharm = False  # TODO fix
Import_study1 = True
# TODO: make global username variable


class ImportTools:
    """
    Class contains tools for importing data tables into database.
    """
    def __init__(self):
        """
        Initializes one ImportTools object.
        """
        # Variables we will add in:
        self.our_added_vars = ['study_name', 'study_year', 'sample_type', 'geo_cord_system', 'utm_cord_system']
        # Table name with bioed:
        self.table_name = "cr"

    @staticmethod
    def read_in_csv(filename, sep="|"):
        """
        Reads in csv "filename" as a pandas dataframe.
        :param filename: string containing file name of the csv file to be imported.
        :param sep: character that separate fields within the csv, default "|"
        :return: pandas dataframe containing data within filename.
        Static method.
        """
        temp_table = pd.read_csv(filename, sep=sep, header=1)
        return temp_table

    @staticmethod
    def execute_query(query):
        """
        Executes mySQL query "query".
        :param query: string containing mySQL query to be executed.
        """
        # If not running within pycharm:
        if not In_pycharm:
            # create the connection to the mysql database
            # If in juypter notebooks, ask user for password. Otherwise proceed using username "test".
            if In_jyptr:
                connection = pymysql.connect(db='group_G', user='anau',
                                             passwd=getpass.getpass(prompt='Password (bioed): ', stream=None),
                                             port=Tunnel.local_bind_port)
            else:
                connection = pymysql.connect(user="test", password="test", db="group_G", port=4253)
            # execute the query and fetch the results:
            with connection.cursor() as cursor:
                cursor.execute(query)
                # retrieve & print your results
                temp = cursor.fetchall()
                for row in temp:
                    print(row)
                connection.commit()
                cursor.close()
            connection.close()

    def create_statement(self):
        """
        Makes create table statement string for master database table.
        :return: string with create table statement.
        """
        # Initialize create table string:
        create_string = f"CREATE TABLE {self.table_name} ( \n"
        create_string += "anid int NOT NULL AUTO_INCREMENT, \n"
        create_string += "study_name VARCHAR(200), \n"
        create_string += "study_year int, \n"
        create_string += "sample_type VARCHAR(200), \n"
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
        # Save file names as a text file:
        with open("column_names.txt", 'w') as my_file:
            for temp in self.our_added_vars:
                my_file.write(f"{temp}\n")
            for temp in full_list:
                my_file.write(f"{temp}\n")
        # Variable types:
        int_variables = ['lab_rep', "cas_rn", 'sig_figs', 'detection_limit', 'reporting_limit']
        # TODO: have two different sizes of decimal values?
        decimal_variables = ['upper_depth', 'lower_depth', 'original_lab_result', 'meas_value',
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
                            'alias_id',
                            'analyte', 'full_name']
        string_variables_long = ['comments']
        # Loop for variables to add (done this way to preserve order)
        for temp in full_list:
            create_string += f"{temp} "
            if temp in string_variables:
                create_string += "VARCHAR(100)"
            elif temp in int_variables:
                create_string += "INT"
            elif temp in decimal_variables:
                create_string += "DECIMAL(40,15)"
            elif temp in date_variables:
                create_string += f"DATETIME"
            elif temp in string_variables_long:
                # TODO is this really best way to do this for comments?
                create_string += "VARCHAR(1000)"
            else:
                print(f"Error: variable missing from data type lists: {temp}")
            create_string += ", \n"
        # Finish statement:
        create_string += "PRIMARY KEY (anid) \n"
        create_string += ") ENGINE = INNODB;"
        # Return create table string:
        return create_string

    def create_table(self):
        """
        Creates data table in bioed. Will drop table if it already exists.
        """
        # Drop table if it already exists:
        drop_existing = f"DROP TABLE IF EXISTS {self.table_name};"
        # Grab create statement:
        create_statement = self.create_statement()
        # If not within pycharm, execute queries:
        if not In_pycharm:
            ImportTools.execute_query(drop_existing)
            ImportTools.execute_query(create_statement)
        print("Create table statement:")
        print(create_statement)

    @staticmethod
    def compare_column_names(columns, ref="column_names.txt"):
        """
        Compares column names to reference column names.
        :param columns: list of column names to check.
        :param ref: text file containing the column names already in the table, 1 name per line.
        :return: tuple of lists: (list of shared columns, list of new columns,
        list of columns missing from master database table).
        If columns are missing from the master database table, that must be resolved before data can be inserted.
        Static method
        """
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
    """
    One object of this class stores templates for each recognized study template.
    """
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
    """
    One object of this class contains variables and methods required to import data from a study into the database.
    Inherits from ImportTools.
    """
    def __init__(self, the_file, study_name, study_year, sample_type, geo_cord_system, utm_cord_system):
        """
        Initializes one ImportStudy object.
        :param the_file: csv or excel file containing study data.
        :param study_name: string containing name to be given to the study within the database.
        :param study_year: integer representing study year.
        :param sample_type: string containing study type.
        :param geo_cord_system: string specifying coordinate system used in x & y coordinate columns.
        :param utm_cord_system: string specifying coordinate system used in x & y utm coordinate columns.
        """
        # Initialize variables from parent:
        super().__init__()
        # Initialize variables:
        self.the_file = the_file
        self.table = self.read_in_csv(filename=self.the_file)
        self.study_name = study_name
        self.study_year = study_year
        self.sample_type = sample_type
        self.geo_cord_system = geo_cord_system
        self.utm_cord_system = utm_cord_system
        self.finish_building_table()  # Finish building table, including columns not included in csv file.
        self.col_names = []  # All column names within study
        self.shared_cols = []  # All shared columns with reference file
        self.new_cols = []  # All new columns that were not present in reference file
        self.miss_cols = []  # All columns that are in the master database table but not present in the current study
        self.insert_statement = ""

    def finish_building_table(self):
        """
        Finishes building pandas dataframe with study data, including variables not included in the input file.
        """
        self.table.insert(0, column="study_name", value=self.study_name)
        self.table.insert(1, column="study_year", value=self.study_year)
        self.table.insert(2, column="sample_type", value=self.sample_type)
        self.table.insert(3, column="geo_cord_system", value=self.geo_cord_system)
        self.table.insert(4, column="utm_cord_system", value=self.utm_cord_system)
        # TODO: handle missing filling in missing columns?
        print("Column names after table built:")
        print(self.table.columns)

    def run_import(self):
        """
        Runs import of current data table.
        """
        # Compare columns of current study to master database table:
        self.check_columns()
        # If there are not missing columns in the master database table, go ahead and insert data:
        if len(self.new_cols) != 0:
            print("There are new columns, table columns must be modified before proceeding!!!!!")
        else:
            # Make insert statement
            self.make_insert_statement()
            # Grab global variable:
            global Tunnel
            # TODO: does this need to be here because other wise times out?
            # Connect with database and execute insert statment:
            if In_jyptr:
                #TODO remove # connect to bioed via an ssh tunnel
                #TODO remove # do NOT include your password, use getpass
                #TODO remove Tunnel = sshtunnel.SSHTunnelForwarder(
                #TODO remove     ('bioed.bu.edu', 22),
                #TODO remove     ssh_username='anau',
                #TODO remove     ssh_password=getpass.getpass(prompt='Password (bu): ', stream=None),
                #TODO remove     remote_bind_address=('localhost', 4253))
                # the password requested here is your kerberos password that you use to access bioed
                # "activate" the ssh tunnel
                #TODO REMOVE  Tunnel.start()
                self.execute_query(self.insert_statement)
                #TODO REMOVE  Tunnel.stop()
        # TODO: modify to raise error?
        pass

    def check_columns(self):
        """
        Compares column names of current data table with master database table.
        Save results in class attributes.
        Prints results.
        """
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

    def make_insert_statement(self, save_to="temp_insert.txt"):
        """
        Makes insert statement string for current data table and saves in file "save_to".
        :param save_to: text file to save insert statement to, default "temp_insert.txt"
        """
        # Initialize insert statment with header:
        insert_string = self.insert_header()
        insert_string += " \nValues "
        # Copy and modify datatable for use in making string:
        temp_table = self.table.copy(deep=True)
        temp_table = temp_table.fillna("Null")
        temp_table = temp_table.values.tolist()
        # For every row in table, make an entry in insert statement:
        for row in temp_table:
            insert_string += "("
            # For every entry in a row:
            for temp in row:
                # If temp is not a int, float, or "Null", surround with quotations:
                if not (isinstance(temp, int) or isinstance(temp, float) or temp == "Null"):
                    temp = f"'{temp}'"
                # If temp is a boolean, not in mySQL format, surround with quotations:
                elif isinstance(temp, bool):
                    temp = f"'{temp}'"
                insert_string += f"{temp}, "
            # Finish off row in insert string:
            insert_string = insert_string[:-2]
            insert_string += "), \n"
        # Finish off insertion string and save in self.insert_statement and to temp_insert.txt
        insert_string = insert_string[:-3]
        insert_string += ";"  # TODO: does floating semicolon work?
        self.insert_statement = insert_string
        # Save insert statement to file:
        # Does this append or overwrite in juyptr?
        with open(save_to, "w") as my_file:
            my_file.write(self.insert_statement)

    def insert_header(self):
        """
        Creates header string for insert statement.
        :return: string containing header info for insert statement.
        """
        # Initialize string:
        build_str = f"INSERT INTO {self.table_name} ("
        # Specify order variables will be specified in insert statement:
        for temp in self.col_names:
            build_str += f"{temp}, "
        # Remove last two values in string & finish building string:
        build_str = build_str[:-2]
        build_str += ")"
        # Return header string:
        return build_str


# main function:
def main():
    """
    Main function to run to run entire program.
    """
    # Grab global variable:
    global Tunnel
    # Set up sshtunnel (for juyptr notebook):
    our_import = ImportTools()
    if In_jyptr:
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
    #TODO remove if In_jyptr:
    #TODO remove     Tunnel.stop()
    # Import study1:
    # Tunnel = None
    if Import_study1:
        study1 = ImportStudy(the_file="Phase 1 Sediment.csv", study_name="Phase1Sediment", study_year=2005,
                             sample_type="Sediment",
                             geo_cord_system="unknown_A1", utm_cord_system="unknown_A2")
        #TODO study1 = ImportStudy(the_file="Phase 1 Sediment SMALL.csv", study_name="Phase1Sediment", study_year=2005,
        #TODO                      sample_type="Sediment",
        #TODO                      geo_cord_system="unknown_A1", utm_cord_system="unknown_A2")
        study1.run_import()
    if In_jyptr:
        Tunnel.stop()


if __name__ == '__main__':
    main()

# TODO: class to pickle data templates

# TODO: use a modfied version of creating temporary SQL tables, but with certain columns forced?
# TODO: class to create temporary SQL tables

# TODO: class to check if data template already exists, and import

# TODO: class to import novel data template & save
# TODO: Function to add new column to already created sql table?
