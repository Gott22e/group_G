#!/usr/local/Python-3.7/bin/python
"""
Program contains classes and functions to create data table and insert rows for the
Upper Columbia River Site database.

Program is designed to be run from within the juyptr notebook "data_import.ipynb".
(This is required for password management and tunneling.)

Updated: 2021/05/02

For issues with this script, contact Allison Nau.
"""

# to change permissions in linus: chmod -R 777

# Booleans to specify what parts of the code to run (only one of In_pycharm, In_jyptr, In_webiste, should be True):
# If In_pycharm is True, will not connect with actual database
# Juypter notebooks is used to get user passwords and connect with database. When code is pulled into Juypter notebooks,
# specify In_jyptr as True (use data_import.ipynb to do so)
# If using script through website, specify In_website as True
In_pycharm = True
In_jyptr = False
In_website = False

# Create partial insert statements? (To save to text. Partial insert statement will be automatically made when necessary
# when connected to database
Partial_insert = False

# Import tools from Mae Rose:
import import_tools_MR as mr

# Import packages
if not In_website:
    import sshtunnel
    import getpass
import pandas as pd

# Don't truncate columns:
pd.set_option('display.max_columns', None)
import pymysql
import pickle
import os
import math
import numpy as np
from io import StringIO
# Requires xlrd, openpyxl for pandas excel support:
import xlrd

# TODO import openpyxl

# TODO: check all metal names are consistent in table, and we don't have some random weird ones

# TODO: Fix where duplicated columns go if one is entirely NULLS
# TODO: (See: river_mile_dup in Phase 2 Sediment Teck data)
# TODO: maybe drop empty columns... or iterate through

# TODO: confirm that the number of rows inserted match the dataset

# TODO deal with NaN, None, Null, etc.
# TODO handle "_dup" pseudo duplicated columns
# TODO: count rows for each study is correct
# TODO: see if rinse blanks made it in
# TODO: "UserWarning: Cannot parse header or footer so it will be ignored
#   warn("""Cannot parse header or footer so it will be ignored""")"
# TODO: determine what are private methods and change

# TODO row count to make sure everything is there
# TODO compare insert string between juyptr and pycharm
# TODO error catching when insert statements don't work (including: when field is too long for database)

# TODO Add INDEXES AT END OF SCRIPT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# TODO INDEXES!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# TODO: convert to cm!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# Global variables:
Tunnel = None
Username = "anau"
Bioed_pw = None
Table_to_use = "cr"  # TODO change


# TODO fix alignment for certain studies
# TODO: add column names in


class NoKnownTemplate(Exception):
    """
    Raised when data file structure is not recognized.
    """
    # TODO: enable this!!! (Instead of the print statments)
    pass


class DatatypeError(Exception):
    """
    Raised when there is a data of the wrong type within a column that has been uploaded to be imported.
    """
    # TODO enable this!!! (Instead of print statements)
    pass


class KnownStudyTemplates:
    """
    One object of this class stores templates for each recognized study template.
    """

    def __init__(self):
        """
        Initializes one KnownStudyTemplates object.
        """
        # Dictionary of study templates:
        self.templates = [
            [['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg',
              'anal_type', 'labsample', 'study_id', 'sample_no', 'sampcoll_id',
              'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
              'composite_type', 'taxon', 'sample_material', 'sample_description',
              'subsamp_type', 'upper_depth', 'lower_depth', 'depth_units',
              'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
              'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units',
              'sig_figs', 'lab_flags', 'qa_level', 'lab_conc_qual', 'validator_flags',
              'detection_limit', 'reporting_limit', 'undetected', 'estimated',
              'rejected', 'greater_than', 'tic', 'reportable', 'alias_id', 'comments',
              'river_mile', 'x_coord', 'y_coord', 'srid']],
            [['study_loc_id', 'location_id', 'principal_doc', 'lab', 'lab_pkg',
              'anal_type', 'labsample', 'study_id', 'sample_no', 'sampcoll_id',
              'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
              'composite_type', 'taxon', 'sample_material', 'sample_description',
              'subsamp_type', 'upper_depth', 'lower_depth', 'depth_units',
              'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
              'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units',
              'sig_figs', 'lab_flags', 'qa_level', 'lab_conc_qual', 'validator_flags',
              'detection_limit', 'reporting_limit', 'undetected', 'estimated',
              'rejected', 'greater_than', 'tic', 'reportable', 'comments',
              'river_mile', 'x_coord', 'y_coord', 'srid'],
             ['location_id', 'principal_doc', 'river_mile', 'utm_x', 'utm_y', 'srid',
              'lat_WGS84_auto_calculated_only_for_mapping',
              'lon_WGS84_auto_calculated_only_for_mapping']],
            [['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg', 'anal_type', 'labsample', 'study_id',
              'sample_no', 'sampcoll_id', 'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
              'composite_type', 'taxon', 'sample_material', 'sample_description', 'subsamp_type', 'upper_depth',
              'lower_depth', 'depth_units', 'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
              'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units', 'sig_figs', 'lab_flags', 'qa_level',
              'lab_conc_qual', 'validator_flags', 'detection_limit', 'reporting_limit', 'undetected', 'estimated',
              'rejected', 'greater_than', 'tic', 'reportable', 'nd_reported_to', 'qapp_deviation', 'nd_rationale',
              'alias_id', 'comments', 'river_mile', 'x_coord', 'y_coord', 'elevation', 'elev_ft'],
             ['location_id', 'river_mile', 'utm_x', 'utm_y', 'elevation', 'elev_unit']],
            [['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg', 'anal_type', 'labsample', 'study_id',
              'sample_no', 'sampcoll_id', 'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
              'composite_type', 'taxon', 'sample_material', 'sample_description', 'subsamp_type', 'upper_depth',
              'lower_depth', 'depth_units', 'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
              'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units', 'sig_figs', 'lab_flags', 'qa_level',
              'lab_conc_qual', 'validator_flags', 'detection_limit', 'reporting_limit', 'undetected', 'estimated',
              'rejected', 'greater_than', 'tic', 'reportable', 'nd_reported_to', 'qapp_deviation', 'nd_rationale',
              'alias_id', 'comments', 'river_mile', 'x_coord', 'y_coord', 'srid'],
             ['location_id', 'principal_doc', 'river_mile', 'utm_x', 'utm_y', 'srid']],
            [['reach_x', 'station', 'lab_sample_id', 'field_id', 'analyte', 'units', 'value', 'reach_y',
              'sample_type_1', 'sampling_coordinates_utm_zone_11_easting',
              'sampling_coordinates_utm_zone_11_northing', 'field_sampling_date',
              'sample_depth_range_in_inches_from_surface']],
            [['sample_id', 'longitude', 'latitude', 'date_collected', 'top_depth', 'bottom_depth', 'dept_unit',
              'analyte', 'units', 'value']]
        ]
        # TODO: generalize template 1 and 2 and 3 together
        print(f"Length of study templates: {len(self.templates)}")  # TODO remove
        # Dictionary of studies and what template to use:
        self.study_temps = {}
        # If study saved templates have been made:
        if os.path.exists("saved_templates"):
            temp = pickle.load(open("saved_templates", "rb"))
            self.study_temps = temp.study_temps
        # TODO expand functionality to handle other KnownStudyTemplate variables

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


class ImportTools:
    """
    Class contains tools for importing data tables into database.
    """
    # Variables to have in create table statement (in addition to our added variables):
    # TODO: check geo reorder worked
    full_list = ['study_name', 'study_year', 'sample_type',
                 'study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg',
                 'anal_type', 'labsample', 'study_id', 'sample_no', 'sampcoll_id',
                 'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
                 'composite_type', 'taxon', 'sample_material', 'sample_description',
                 'subsamp_type', 'upper_depth', 'lower_depth', 'depth_units',
                 'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
                 'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units', "value_note",
                 'sig_figs', 'lab_flags', 'qa_level', 'lab_conc_qual', 'validator_flags',
                 'detection_limit', 'reporting_limit', 'undetected', 'estimated',
                 'rejected', 'greater_than', 'tic', 'reportable', 'alias_id',
                 'nd_reported_to', 'qapp_deviation', 'nd_rationale', 'comments',
                 'river_mile', 'river_mile_dup',
                 'geo_cord_system', 'x_coord', 'y_coord',
                 'utm_cord_system', 'utm_x', 'utm_y', 'srid', 'srid_dup',
                 'lat_WGS84_auto_calculated_only_for_mapping', 'lon_WGS84_auto_calculated_only_for_mapping',
                 'principal_doc_location', 'elevation', 'elev_unit', 'elevation_dup', 'elev_ft',
                 'reach', 'station']

    def __init__(self):
        """
        Initializes one ImportTools object.
        """
        # Variables we will add in:
        self.our_added_vars = ['study_name', 'study_year', 'sample_type', 'geo_cord_system', 'utm_cord_system']
        # Table name with bioed:
        global Table_to_use
        self.table_name = Table_to_use
        # Study templates:
        self.known_templates = KnownStudyTemplates()
        # Variable types:
        self.int_variables = ['study_year', 'sig_figs', 'detection_limit', 'reporting_limit']
        # TODO: have two different sizes of decimal values?
        self.decimal_variables = ['upper_depth', 'lower_depth', 'original_lab_result', 'meas_value',
                                  'river_mile', 'river_mile_dup', 'x_coord', 'y_coord', 'srid', 'srid_dup',
                                  'utm_x', 'utm_y', 'lat_WGS84_auto_calculated_only_for_mapping',
                                  'lon_WGS84_auto_calculated_only_for_mapping', 'elev_ft', 'elevation', 'elevation_dup']
        self.date_variables = ['sample_date']
        # TODO: is lab_conc_qual really string?
        # TODO: is cas_rn OK as a string?
        # TODO is lab_rep OK as string?
        # TODO: booleans currently as strings: 'undetected', 'estimated',
        #        'rejected', 'greater_than', 'tic', 'reportable',
        self.string_variables = ['study_name', 'sample_type', 'geo_cord_system', 'utm_cord_system',
                                 'study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg', 'anal_type',
                                 'labsample', 'analyte', 'full_name', 'principal_doc_location',
                                 'study_id', 'sample_no', 'sampcoll_id', 'sum_sample_id', 'sample_id', "study_element",
                                 'composite_type', 'taxon', 'sample_material', 'subsamp_type',
                                 'depth_units', 'material_analyzed', 'method_code', 'meas_basis', 'units', 'lab_flags',
                                 'qa_level', 'lab_conc_qual', 'validator_flags',
                                 'undetected', 'estimated',
                                 'rejected', 'greater_than', 'tic', 'reportable',
                                 'alias_id', "cas_rn", 'lab_rep',
                                 'qapp_deviation', 'nd_reported_to', 'elev_unit', 'reach', 'station', "value_note"]
        self.string_variables_long = ['comments', 'sample_description', 'nd_rationale']
        # Added indices:
        self.table_index = {"analyte_idx": "analyte", "date_idx": "sample_date"}

    @staticmethod
    def read_in_csv(filename, sep=","):
        """
        Reads in csv "filename" as a pandas dataframe.
        :param filename: string containing file name of the csv file to be imported.
        :param sep: character that separate fields within the csv, default ","
        :return: pandas dataframe containing data within filename.
        Static method.
        """
        # Is first row column names or second row?
        temp_table_v1 = pd.read_csv(filename, sep=sep, header=1)
        header_v1 = set(temp_table_v1.columns)
        intersection_v1 = header_v1.intersection(ImportTools.full_list)
        print(intersection_v1)
        temp_table_v2 = pd.read_csv(filename, sep=sep, header=0)
        header_v2 = set(temp_table_v2.columns)
        intersection_v2 = header_v2.intersection(ImportTools.full_list)
        print(intersection_v2)
        if len(intersection_v1) > 1:
            temp_table = temp_table_v1
        elif len(intersection_v2) > 1:
            temp_table = temp_table_v2
        else:
            print("Can't identify row column names are in")
            temp_table = None
        temp_table.drop_duplicates(inplace=True)  # TODO: does this work properly?
        return temp_table

    @staticmethod
    def read_in_excel(filename, sample_type):
        """
        Reads in excel file "filename", returning a dictionary where the keys are the sheet names, and
        the values are a pandas dataframe representing one sheet of data.
        Will skip sheets named "SQL used" or "history".
        :param filename: excel file to read in.
        :param sample_type: type of sample, used to determine what excel sheets can be skipped. (e.g. "Sediment")
        :return: dictionary of pandas dataframes containing "filename" data.
        """
        table_dict = {}
        whole = pd.ExcelFile(filename)
        sheets = whole.sheet_names
        for sheet in sheets:
            if sheet != "SQL used" and sheet != "history" and \
                    not (sheet == "field measurements" and sample_type == "Sediment"):
                print(f"Reading in sheet: {sheet}")
                if filename.endswith("xlsx"):
                    table_dict[sheet] = pd.read_excel(filename, sheet_name=sheet, engine="openpyxl")
                elif filename.endswith("xls"):
                    table_dict[sheet] = pd.read_excel(filename, sheet_name=sheet)
                else:
                    print("File name extension is not recognized")
            else:
                print(f"Skipping sheet: {sheet}")
        # Drop duplicate rows:  # TODO Does this work well enough?
        for sheet in table_dict:
            table_dict[sheet].drop_duplicates(inplace=True)
        return table_dict

    @staticmethod
    def read_in_dict_csvs(my_dict, sample_type, sep=","):
        """
        Reads in a dictionary of csv filenames, created a dictionary of pandas dataframes.
        :param my_dict: dictionary where the values are csv filenames, and the key represent the sheet it came from in
        the excel file.
        :param sample_type: string representing sample type. Should be in: ["Sediment"]
        :param sep: delimiter used in csv file. Default "," .
        :return: dictionary of pandas dataframes.
        """
        # TODO: make sample_type enum?
        table_dict = {}
        for sheet, filename in my_dict.items():
            if sheet != "SQL used" and sheet != "history" and \
                    not (sheet == "field measurements" and sample_type == "Sediment"):
                print(f"Reading in sheet: {sheet}")
                table_dict[sheet] = ImportTools.read_in_csv(filename, sep=sep)
            else:
                print(f"Skipping sheet: {sheet}")
        # Drop duplicate rows:  # TODO Does this work well enough?
        for sheet in table_dict:
            table_dict[sheet].drop_duplicates(inplace=True)
        return table_dict

    @staticmethod
    def read_in_dict_strings(my_dict, sample_type):
        """
        Reads in a dictionary of strings representing csv files, created a dictionary of pandas dataframes.
        :param my_dict: dictionary where the values are strings representing csv files, and the key represent the
        sheet it came from in the excel file.
        :param sample_type: string representing sample type. Should be in: ["Sediment"].
        :return: dictionary of pandas dataframes.
        """
        table_dict = {}
        sheets = my_dict.keys()
        for sheet in sheets:
            if sheet != "SQL used" and sheet != "history" and \
                    not (sheet == "field measurements" and sample_type == "Sediment"):
                print(f"Reading in sheet: {sheet}")
                my_string = StringIO(my_dict[sheet])
                table_dict[sheet] = pd.read_csv(my_string)
            else:
                print(f"Skipping sheet: {sheet}")
        # Drop duplicate rows:  # TODO Does this work well enough?
        for sheet in table_dict:
            table_dict[sheet].drop_duplicates(inplace=True)
        return table_dict

    @staticmethod
    def execute_query(query):
        """
        Executes mySQL query "query".
        :param query: string containing mySQL query to be executed.
        """
        # TODO: handle error catching, including when columns contain values that are too long or are the wrong type
        # TODO: probably needs to be a return error statement?
        # If not running within pycharm:
        if not In_pycharm:
            # create the connection to the mysql database
            # If in juypter notebooks, ask user for password. Otherwise proceed using username "test".
            if In_website:
                connection = pymysql.connect(user="test", password="test", db="group_G", port=4253)
            elif In_jyptr:
                global Bioed_pw
                connection = pymysql.connect(db='group_G', user=Username,
                                             passwd=Bioed_pw,
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
        #TODO remove create_string += "study_name VARCHAR(200), \n"
        #TODO remove create_string += "study_year int, \n"
        #TODO remove create_string += "sample_type VARCHAR(200), \n"
        #TODO remove create_string += "geo_cord_system VARCHAR(100), \n"
        #TODO remove create_string += "utm_cord_system VARCHAR(100), \n"
        # Save file names as a text file:
        with open("column_names.txt", 'w') as my_file:
            #TODO remove for temp in self.our_added_vars:
            #TODO remove     my_file.write(f"{temp}\n")
            for temp in ImportTools.full_list:
                my_file.write(f"{temp}\n")
        # Loop for variables to add (done this way to preserve order)
        for temp in ImportTools.full_list:
            create_string += f"{temp} "
            if temp in self.string_variables:
                create_string += "VARCHAR(100)"
            elif temp in self.int_variables:
                create_string += "INT"
            elif temp in self.decimal_variables:
                create_string += "DECIMAL(40,15)"
            elif temp in self.date_variables:
                create_string += f"DATETIME"
            elif temp in self.string_variables_long:
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

    def create_index_statement(self):
        """
        Returns mySQL statement (string) that creates indices in self.table_index, if the index does not already exist.
        :return: string representing above query.
        """
        statement = ""
        for idx, con in self.table_index.items():
            statement += f"alter table {self.table_name} add index if not exists {idx} ({con}); \n"
        if not In_website:
            print("Create Index statement:")
            print(statement)
        return statement

    def create_drop_index_statement(self):
        """
        Returns mySQL statement (string) that drops indices in self.table_index, if the index exists.
        :return: string representing above query.
        """
        statement = ""
        for idx, con in self.table_index.items():
            statement += f"alter table {self.table_name} drop index if exists {idx}; \n"
        if not In_website:
            print("Create drop index statement:")
            print(statement)
        return statement

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
        with open("create_table.txt", "w") as my_file:
            my_file.write(create_statement)

    @staticmethod
    def compare_column_names(columns, ref="column_names.txt"):
        """
        Compares column names to reference column names.
        :param columns: list of column names to check.
        :param ref: text file containing the column names already in the table, 1 name per line.
        :return: tuple of lists: (list of shared columns, list of new columns,
        list of columns missing from master database table).
        If columns are missing from the master database table, that must be resolved before data can be inserted.
        Static method.
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
    def clean_col_names(df):
        """
        Cleans up column names in pandas dataframe "df", returning the suggested new names as a list.
        :param df: pandas dataframe.
        :return: list of column names.
        """
        # Current list of column names:
        cols = list(df.columns)
        new_names = []
        # Name swaps to make:
        dict_of_swaps = {"lat_wgs84_auto_calculated_only_for_mapping": "lat_WGS84_auto_calculated_only_for_mapping",
                         "lon_wgs84_auto_calculated_only_for_mapping": "lon_WGS84_auto_calculated_only_for_mapping"}
        # Replace undesired characters with _
        for col in cols:
            new_col = col.lower()
            new_col = new_col.strip()
            for char in ["(", ")", ":", ";", " ", "-"]:
                new_col = new_col.replace(char, "_")
            for _ in range(5):
                new_col = new_col.replace("__", "_")
            if new_col in dict_of_swaps:
                new_col = dict_of_swaps[new_col]
            new_col = new_col.strip("_")
            new_names.append(new_col)
        return new_names

    @staticmethod
    def convert_to_cm(df, cols=["upper_depth", "lower_depth"], unit_col="depth_units"):
        """
        Converts units (in, m, ft) to cm.
        :param df: dataframe to be converted
        :param cols: list of columns of numerical values to change, should all use the same units.
        :param unit_col: column containing units value.
        :return: dataframe with units converted.
        """
        # TODO
        # TODO make lower case units
        # Make units column lower case and stripped of leading and trailing white space:
        df[unit_col] = df[unit_col].str.lower()
        df[unit_col] = df[unit_col].str.strip()
        # Equivalency dictionary:
        equivalent = {"inches": "in", "inch": "in",
                      "centimeter": "cm", "centimeters": "cm",
                      "meter": "m", "meters": "m",
                      "feet": "ft", "foot": "ft"}
        # Conversion dictionary:
        convert_cm = {"cm": 1,
                      "m": 1 / 100,
                      "in": 2.54,
                      "ft": 30.48}
        for i in df.index:
            #TODO remove if df[unit_col][i] in equivalent:
            #TODO remove     df[unit_col][i] = equivalent[df[unit_col][i]]
            #TODO remove if df[unit_col][i] in convert_cm:
            #TODO remove     for col in cols:
            #TODO remove         df[col][i] = df[col][i]/convert_cm[df[unit_col][i]]
            if df.loc[i, unit_col] in equivalent:
                df.loc[i, unit_col] = equivalent[df.loc[i, unit_col]]
            if df.loc[i, unit_col] in convert_cm:
                for col in cols:
                    df.loc[i, col] = df.loc[i, col] * convert_cm[df.loc[i, unit_col]]
                df.loc[i, unit_col] = "cm"
        return df


class ImportStudy(ImportTools):
    """
    One object of this class contains variables and methods required to import data from a study into the database.
    Inherits from ImportTools.
    """

    def __init__(self, the_input, study_name, study_year, sample_type, geo_cord_system, utm_cord_system,
                 is_csv=False, is_excel=False, is_dict_strings=False, is_dict_filenames=False, special_header=False,
                 sep=",",
                 special_col_names_expand=None,
                 special_cols_with_values=None, special_add_units_to_cols=None, special_merge_with=None):
        """
        Initializes one ImportStudy object.
        NOTE: files with a special header CANNOT be accepted as an excel file.
        :param the_input: csv or excel file containing study data. Can also be a dictionary of csv filenames, where
        the key represents the expected sheet name (to preserve the relationships between sheets), and the value being
        a string with the csv file name. Can also be a dictionary of strings, where the key represents the expected
        sheet name (to preserve the relationships between sheets) and the string is a csv file copied over from a
        text editor (i.e. a string representing a CSV file).
        Expect sheet names like: ["labresults", "locations", "chemistry", "location and depth"]
        :param study_name: string containing name to be given to the study within the database.
        :param study_year: integer representing study year.
        :param sample_type: string containing study type.
        :param geo_cord_system: string specifying coordinate system used in x & y coordinate columns.
        :param utm_cord_system: string specifying coordinate system used in x & y utm coordinate columns.
        :param is_csv: boolean specifying if input file is a csv.
        :param is_excel: boolean specifying if input file is an Excel workbook.
        :param is_dict_strings: boolean specifying if input file is a dictionary of strings representing csv files.
        :param is_dict_filenames: boolean specifying if input file is a dictionary of csv filenames.
        :param special_header: boolean specifying if there is formatting in header of excel file that
        must be adhered to.
        If special_header is True, then there are some additional parameters:
        :param special_col_names_expand: for data that needs to be rearranged from column to row format, these
        are the new column names. For example, if analytes represent multiple columns and are being converted from
        wide format to long format, the new column names might be "Analyte", "Units", "Value".
        :param special_cols_with_values: list representing the indexes of columns that contain numerical data to convert
        to row format.
        :param special_add_units_to_cols: dictionary of units (keys) associated with the columns being arranged
        into rows and a list of column indexes those units apply to (values).
        e.g. {"%": list(range(4, 16)), "(mg/kg)": list(range(16, 26))}
        :param special_merge_with: dictionary containing information needed to merge with the primary input sheet. The
        key is the sheet name to be added to the primary sheet, and the value is a list of column names for the merge
        to be performed on. The primary sheet should not be included in this dictionary.
        e.g. {"location and depth": ["Station", "Lab Sample ID", "Field ID"]}.
        """
        # Initialize variables from parent:
        super().__init__()
        # Initialize variables:
        self.the_input = the_input
        self.is_csv = is_csv
        self.is_excel = is_excel
        if not In_website:
            self.is_dict_strings = is_dict_strings
        else:
            self.is_dict_strings = True
        # TODO: catch if all are False input types
        self.is_dict_filenames = is_dict_filenames
        self.sep = sep
        self.study_name = study_name
        self.study_year = study_year
        self.sample_type = sample_type
        self.geo_cord_system = geo_cord_system
        self.utm_cord_system = utm_cord_system
        self.special_header = special_header
        self.special_col_names_expand = special_col_names_expand
        self.special_cols_with_values = special_cols_with_values
        self.special_add_units_to_cols = special_add_units_to_cols
        self.special_merge_with = special_merge_with
        self.use_template = None
        self.col_names_by_sheet = {}
        self.col_names = []  # All column names within study
        self.shared_cols = []  # All shared columns with reference file
        self.new_cols = []  # All new columns that were not present in reference file
        self.miss_cols = []  # All columns that are in the master database table but not present in the current study
        self.insert_statement = ""
        self.insert_statement_list = []  # List of alternative insert statements to be inserted one at a time
        self.found_template = False
        # Read in study (self.table will be a pandas dataframe if is_csv is True, otherwise will be
        # a list of pandas dataframes:
        self.table = self.read_in_study(filename=self.the_input)
        # Compare column headers with other studies to see what templates make sense:
        self.compare_with_other_studies()
        if self.found_template:  # If template was found, proceed
            # If study was an excel file, combine into one table, according to template in self.use_template
            if not isinstance(self.table, pd.DataFrame) or self.special_header:  # TODO working properly?
                self.combine_sheets_rearranges()
            # Convert units:
            self.table = ImportTools.convert_to_cm(self.table)
            # If template was found, go ahead and add columns with the study info:
            if self.use_template is not None:
                self.finish_building_table()  # Finish building table, including added columns with study info
        # Compare columns of current study to master database table and references:
        self.check_columns()

    def read_in_study(self, filename):
        """
        Read in data stored in "filename".
        :param filename: csv or excel file containing study data. Can also be a dictionary of csv filenames, where
        the key represents the expected sheet name (to preserve the relationships between sheets), and the value being
        a string with the csv file name. Can also be a dictionary of strings, where the key represents the expected
        sheet name (to preserve the relationships between sheets) and the string is a csv file copied over from a
        text editor (i.e. a string representing a CSV file).
        :return: dataframe (csv) or list of dataframes (excel) containing study data.
        """
        # If there is a special header, and a dictionary of filenames was received:
        if self.special_header and self.is_dict_filenames:
            table = self.read_in_special_dict_filename()
            table.columns = self.clean_col_names(table)
            self.col_names_by_sheet["sheet1"] = table.columns
        # TODO: handle dict of strings with special header
        # If there is a special header, and a csv was received:
        elif self.special_header and self.is_csv:
            table = self.read_in_special_csv()
            # TODO handle non-csvs seps?
            table.columns = self.clean_col_names(table)
            self.col_names_by_sheet["sheet1"] = table.columns
            #TODO print(table.head(n=5))  # TODO remove
            #TODO table.to_csv("temp2.csv")
        # If input in a csv:
        elif self.is_csv:  # TODO: Template 0?
            table = ImportTools.read_in_csv(filename, sep=self.sep)
            table.columns = self.clean_col_names(table)  # TODO: does this work for csv?
            self.col_names_by_sheet["sheet1"] = table.columns
        # If input is an excel file:
        elif self.is_excel:
            table = ImportTools.read_in_excel(filename, self.sample_type)
            for t in table:
                table[t].columns = self.clean_col_names(table[t])
                self.col_names_by_sheet[t] = table[t].columns
        # If input is a dictionary of strings:
        elif self.is_dict_strings:
            table = ImportTools.read_in_dict_strings(filename, self.sample_type)
            for t in table:
                table[t].columns = self.clean_col_names(table[t])
                self.col_names_by_sheet[t] = table[t].columns
            # If dictionary is only one entry:
            if len(table) == 1:
                for t in table:
                    table = table[t]
        # If input is a dictionary of csv filenames:
        elif self.is_dict_filenames:
            table = ImportTools.read_in_dict_csvs(filename, self.sample_type, sep=self.sep)
            for t in table:
                table[t].columns = self.clean_col_names(table[t])
                self.col_names_by_sheet[t] = table[t].columns
            # If dictionary is only one entry:
            if len(table) == 1:
                for t in table:
                    table = table[t]
        else:
            print("Must specify input type (is_csv, is_excel, is_dict_strings, is_dict_filenames")
            print("Must give files with a special header (merged columns, analytes arranged by column, etc.) as")
            print("either a csv or a dictionary of csv filenames.")
        return table

    def read_in_special_csv(self):
        """
        Reads in a single csv, when the header has special requirements (i.e. analytes are arranged by
        column.)
        :return: dataframe containing data stored in csv file.
        """
        temp = mr.allisort(fileIn=self.the_input,
                           keys=self.special_col_names_expand,
                           values=self.special_cols_with_values,
                           add=self.special_add_units_to_cols)
        table = temp.DF
        return table

    def read_in_special_dict_filename(self):
        """
        Reads in dictionary of csv filenames, when the header has special requirements (i.e. analytes are arranged by
        column.)
        :return: dataframe containing data stored in csv files.
        """
        new_merge_dict = {}
        table = None
        for key in self.special_merge_with:
            new_merge_dict[self.the_input[key]] = self.special_merge_with[key]
            # TODO catch errors
        main_file = set(self.the_input) - set(self.special_merge_with)
        if len(main_file) != 1:
            print("Special Merge Requires that files in addition to the main file each be declared.")
            print("Main file should only be list in the input dictionary.")
        else:
            main_file = list(main_file)[0]
            main_file = self.the_input[main_file]
            temp = mr.allisort(fileIn=main_file,
                               keys=self.special_col_names_expand,
                               values=self.special_cols_with_values,
                               merge=new_merge_dict, add=self.special_add_units_to_cols)
            table = temp.DF
            if "Value" in table.columns:
                table.dropna(axis=0, subset=["Value"], inplace=True)
                table.reset_index(drop=True)
        return table

    def combine_sheets_rearranges(self):
        """
        Combines sheets that were stored in study's excel data file, according to template.
        Also handles a single sheet IF data needs to be rearranged or modified.
        This should not be used if template is unknown.
        (Special header files should already be combined into one dataframe prior to this step.)
        """
        template = self.use_template
        temp_table = None
        print(f"Template to use: {template}")
        if template == 1 or template == 2 or template == 3:
            temp_table = self.template1_clean()
        elif template == 4:
            temp_table = self.template4_clean()
        elif template == 5:
            temp_table = self.template5_clean()
        else:  # If template is not known:
            print("Not recognized template study")
            for col, names in self.col_names_by_sheet.items():
                shared_cols, new_cols, miss_cols = self.compare_column_names(names)
                print(f"For sheet {col}:")
                print("\tShared columns:")
                print(f"\t{shared_cols}")
                print("\tNew columns:")
                print(f"\t{new_cols}")
                print("\tMissing columns:")
                print(f"\t{miss_cols}")
        if temp_table is not None:  # If temp_table was successfully built
            print("New columns:")
            print(temp_table.columns)
            self.table = temp_table

    def template1_clean(self):
        """
        Cleans up studies that follow template1, template2, or template3 and combines into one dataframe.
        :return: one cleaned up pandas dataframe.
        """
        # TODO: need to confirm this is OK renamed column for all studies of this template
        # Rename columms that are duplicated on different sheets, but are not being used as part of the join:
        if "labresults" in self.table:
            self.table["labresult"] = self.table.pop("labresults")
        if "lab results" in self.table:
            self.table["labresult"] = self.table.pop("lab results")
        if "lab_results" in self.table:
            self.table["labresult"] = self.table.pop("lab_results")
        self.table["labresult"].rename({"river_mile": "river_mile_dup",
                                        "srid": "srid_dup"}, axis='columns', inplace=True)
        self.table["locations"].rename({"principal_doc": "principal_doc_location"}, axis='columns', inplace=True)
        if self.use_template == 2:
            self.table["labresult"].rename({"elevation": "elevation_dup"}, axis='columns', inplace=True)
        # Merge duplicated location information:
        # (group by everything BUT principal_doc_location
        # Handle partially duplicated rows using:
        # https://www.geeksforgeeks.org/select-all-columns-except-one-given-column-in-a-pandas-dataframe/
        # https://stackoverflow.com/questions/36271413/pandas-merge-nearly-duplicate-rows-based-on-column-value/45088911
        print(list(self.table["locations"].columns.values))
        group_by_cols = list(self.table["locations"].columns.values)
        if "principal_doc_location" in group_by_cols:
            # Make sure principal_doc_location is a string:
            self.table["locations"]["principal_doc_location"] = self.table["locations"][
                "principal_doc_location"].astype(str)
            group_by_cols.remove("principal_doc_location")
            # TODO: go back to group by all other columns:
            # TODO self.table["locations"] = self.table["locations"].groupby(group_by_cols)['principal_doc_location'].apply(', '.join).reset_index()
            self.table["locations"] = self.table["locations"].groupby(["location_id"])['principal_doc_location'].apply(
                ', '.join).reset_index()
        print(self.table["locations"].head(n=5))
        # Merge sheets:
        temp_table = pd.merge(self.table['labresult'], self.table['locations'], on=["location_id"], how="left")
        temp_table = self.clean_numeric_cols_of_nulls(temp_table)
        # Clean up analyte values
        temp_table["analyte"] = temp_table["analyte"].str.replace("TOC", "Carbon_org", regex=False)
        # Copy river_mile_dup if other column doesn't exist:
        if "river_mile" not in temp_table:
            temp_table["river_mile"] = temp_table["river_mile_dup"]
        return temp_table

    def template4_clean(self):
        """
        Cleans up studies that follow template 4.
        :return: cleaned up dataframe. This dataframe is already stored in self.table.
        """
        # Drop duplicated number:
        self.table.drop("reach_y", axis=1, inplace=True)
        # Rename columns:
        change_dict = {"reach_x": "reach",
                       "field_sampling_date": "sample_date",
                       "sampling_coordinates_utm_zone_11_easting": "utm_x",
                       "sampling_coordinates_utm_zone_11_northing": "utm_y",
                       "sample_type_1": "sample_description",
                       "lab_sample_id": "sample_no",
                       "field_id": "sampcoll_id",
                       "value": "meas_value"}
        for key in change_dict:
            if key in self.table:
                self.table[change_dict[key]] = self.table.pop(key)
        # Clean up analyte values
        self.table["analyte"] = self.table["analyte"].str.replace("Total Organic Carbon", "Carbon_org", regex=False)
        # Break apart sample depth column:
        if "sample_depth_range_in_inches_from_surface" in self.table:
            temp = pd.DataFrame(
                self.table["sample_depth_range_in_inches_from_surface"].str.split("-", n=1, expand=True))
            self.table["upper_depth"] = temp[0]
            self.table["lower_depth"] = temp[1]
            self.table.drop(columns=["sample_depth_range_in_inches_from_surface"], inplace=True)
            for col in ["upper_depth", "lower_depth"]:
                self.table[col] = pd.to_numeric(self.table[col])
            self.table["depth_units"] = "in"
        # Convert date column to date format:
        self.table["sample_date"] = pd.to_datetime(self.table["sample_date"])
        # Handle values listed as <0.5:
        self.table["value_note"] = "Null"
        self.table.loc[self.table["meas_value"] == "<0.5", "value_note"] = "<0.5"
        self.table.loc[self.table["meas_value"] == "<0.5", "meas_value"] = 0
        # Handle cells that should be empty but instead have "--"
        self.table.loc[self.table["meas_value"] == "--", "meas_value"] = ""
        self.table["meas_value"] = pd.to_numeric(self.table["meas_value"])
        # Copy x and y coord over if needed
        if "x_coord" not in self.table and "y_coord" not in self.table:
            self.table["x_coord"] = self.table["utm_x"]
            self.table["y_coord"] = self.table["utm_y"]
            self.geo_cord_system = self.utm_cord_system
        return self.table

    def template5_clean(self):
        """
        Cleans up studies that follow template 5.
        :return: cleaned up dataframe. This dataframe is already stored in self.table.
        """
        # Change column names:
        change_dict = {"date_collected": "sample_date",
                       "top_depth": "upper_depth",
                       "bottom_depth": "lower_depth",
                       "dept_unit": "depth_units",
                       "longitude": "x_coord",
                       "latitude": "y_coord",
                       "value": "meas_value"}
        for key in change_dict:
            if key in self.table:
                self.table[change_dict[key]] = self.table.pop(key)
        # Clean analyte names
        self.table["analyte"] = self.table["analyte"].str.replace(" (mg/kg)", "", regex=False)
        self.table["analyte"] = self.table["analyte"].str.replace(" (mg/Kg)", "", regex=False)
        print(self.table["analyte"])
        self.table["analyte"] = self.table["analyte"].str.replace("Total Organic\nCarbon (%)", "Carbon_org",
                                                                  regex=False)
        # Convert date column to date format:
        self.table["sample_date"] = pd.to_datetime(self.table["sample_date"])
        # Break apart values with flags:
        temp = pd.DataFrame(self.table["meas_value"].str.split(expand=True))
        self.table["meas_value_temp"] = temp[0]
        self.table["lab_flags"] = temp[1]
        self.table["meas_value_temp"].fillna(self.table["meas_value"], inplace=True)
        self.table["meas_value"] = self.table["meas_value_temp"]
        self.table.drop(columns=["meas_value_temp"], inplace=True)
        self.table["meas_value"] = pd.to_numeric(self.table["meas_value"])
        # Drop rows without meas_value:
        self.table.dropna(subset=["sample_id", "meas_value"], inplace=True, how="all")
        return self.table

    def clean_numeric_cols_of_nulls(self, df, missing="Unk"):
        """
        Removes string parameter "missing" from entries in dataframe "df", only from columns that are expected to
        contain numeric data. Used to replace null/na values with
        the mySQL preferred "Null".
        :param df: dataframe to replace "missing" values in.
        :param missing: string representing the value to be replaced.
        :return: cleaned dataframe.
        """
        # TODO: do comparison in a way that isn't case sensitive?
        # Remove nulls in numerical columns:
        cols = self.int_variables + self.decimal_variables
        for col in cols:
            try:
                df[col].replace({missing: "Null"}, inplace=True)
            except KeyError:
                print(f"No column named: {col}")
            except TypeError:  # TODO check type of column rather than catch
                print(f"Can't make numeric to string comparison in numeric pandas column {col}")
        return df

    def finish_building_table(self):
        """
        Finishes building pandas dataframe with study data, including variables not included in the input file.
        """
        self.table.insert(0, column="study_name", value=self.study_name)
        self.table.insert(1, column="study_year", value=self.study_year)
        self.table.insert(2, column="sample_type", value=self.sample_type)
        self.table.insert(3, column="geo_cord_system", value=self.geo_cord_system)
        self.table.insert(4, column="utm_cord_system", value=self.utm_cord_system)
        print("Column names after table built:")
        print(self.table.columns)

    def run_import(self, drop_index=True, replace_index=True):
        """
        Runs import of current data table.
        TODO
        """
        # Get index statements:
        create_statement = self.create_index_statement()
        drop_statement = self.create_drop_index_statement()
        # If study template was found:
        if self.found_template:
            # If there are not missing columns in the master database table, go ahead and insert data:
            if len(self.new_cols) != 0:
                print("There are new columns, table columns must be modified before proceeding!!!!!")
            else:
                # Make insert statement
                master_statement = self.make_insert_statement()
                # Grab global variable:
                global Tunnel
                # Connect with database and execute insert statement:
                if In_jyptr or In_website:
                    # Drop indices:
                    if drop_index:
                        self.execute_query(query=drop_statement)
                    # Run insertion:
                    if len(self.table.index) <= 5000:
                        self.execute_query(self.insert_statement)
                    else:
                        smaller_statements = self.make_smaller_insert_statements(master_statement=master_statement,
                                                                                 perfile=2000)
                        for query in smaller_statements:
                            self.execute_query(query=query)
                    # Add indices back:
                    if replace_index:
                        self.execute_query(query=create_statement)
            # TODO: modify to raise error?
            # Save current known templates:
            if not In_website:
                pickle.dump(self.known_templates, open("saved_templates", "wb"))
        else:
            print("Cannot import study if template is unknown")

    def _check_columns(self):
        """
        Compares column names of current data table with master database table.
        Save results in class attributes.
        Prints results.
        Private method to be used by check_columns.
        Note: if template hadn't been found it will store the last sheet column information in tables.
        """
        print("COLUMN NAMES:")
        print(self.col_names)
        self.shared_cols, self.new_cols, self.miss_cols = self.compare_column_names(self.col_names)
        print("Columns shared with the reference:")
        print(self.shared_cols)
        print("Columns that are new:")
        print(self.new_cols)
        print("Columns that are missing:")
        print(self.miss_cols)
        # Check column datatypes:
        if not isinstance(self.table, dict):
            self.column_types(self.table)

    def check_columns(self):
        """
        Compares column names of current data table with master database table.
        Save results in class attributes.
        Prints results.
        Note: if template hadn't been found it will store the last sheet column information in tables.
        """
        if isinstance(self.table, dict):
            temp_list = []
            print(f"Combining column names for all sheets")
            for key in self.table:
                new_columns = list(self.table[key].columns)
                temp_list.append(new_columns)
                print(f"For sheet '{key}', column names:")
                print(new_columns)
            for sublist in temp_list:
                for item in sublist:
                    self.col_names.append(item)
        else:
            self.col_names = list(self.table.columns)
        self._check_columns()

    def compare_with_other_studies(self):
        """
        Compares column names in the different sheets (or single sheet) of excel/csv file, checking to see
        if a template made for a previous study can be used.
        """
        self.found_template = False
        num_sheets_in_study = len(self.col_names_by_sheet)
        # Check if study has been loaded in previously:
        if self.study_name in self.known_templates.study_temps:
            self.use_template = self.known_templates.study_temps[self.study_name]
            self.found_template = True
            print(f"Study has been seen before, use template #: {self.use_template}")
        # If study hasn't been loaded previous, check if column names in all relevant sheets match a known template:
        # (Will not work correctly if there are duplicate sheets within an excel file).
        else:
            for i in range(len(self.known_templates.templates)):
                matching_sheets = 0
                for sheet1 in self.col_names_by_sheet:
                    for sheet2 in self.known_templates.templates[i]:
                        if set(self.col_names_by_sheet[sheet1]) == set(sheet2):
                            matching_sheets += 1
                if matching_sheets == num_sheets_in_study:
                    self.found_template = True
                    self.use_template = i
                    self.known_templates.study_temps[self.study_name] = i
                    print(f"Study matching template found, template #: {i}")
                    break
            if not self.found_template:
                print("STUDY TEMPLATE NOT FOUND, must be created")

    @staticmethod
    def column_types(df):
        """
        Checks and print column datatypes in pandas dataframe "df".
        :param df:
        :return: tuple (mixed_bool, list_of_mix). "mixed_bool" is True if there are mixed/string datatypes present
        (pandas object datatype). "list_of_mix" contains list of columns that contain pandas object datatypes.
        """
        # Any mixed datatypes present?
        # TODO (Strings also caught in this?)
        mixed_bool = False
        list_of_mix = []
        print("Datatypes in dataframe:")
        for (name, data) in df.iteritems():
            print(f"Column Name: {name}")
            my_dtype = df.dtypes[name]
            print(f"Datatype: {my_dtype}")
            if my_dtype == "object":
                mixed_bool = True
                list_of_mix.append(name)
        print(f"Mixed column datatypes present in dataset: {mixed_bool}")
        print(f"Columns with mixed datatypes present:")
        print(list_of_mix)
        # TODO: raise error with mismatches
        df.info()
        return mixed_bool, list_of_mix

    # TODO: finish implementing this column type function

    def make_insert_statement(self, save_to=None):
        """
        Makes insert statement string for current data table and saves in file "save_to".
        :param save_to: text file to save insert statement to, default self.study_name + "_temp_insert.txt"
        :return: file name that master insert statement was saved to.
        """
        # Save file to:
        if save_to is None:
            save_to = self.study_name + "_temp_insert.txt"
        # Initialize insert statement with header:
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
                    temp = f'"{temp}"'
                # If temp is a boolean, not in mySQL format, surround with quotations:
                elif isinstance(temp, bool):
                    temp = f'"{temp}"'
                insert_string += f"{temp}, "
            # Finish off row in insert string:
            insert_string = insert_string[:-2]
            insert_string += "), \n"
        # Finish off insertion string and save in self.insert_statement and to temp_insert.txt
        insert_string = insert_string[:-3]
        insert_string += ";"
        self.insert_statement = insert_string
        # Save insert statement to file:
        # TODO: Does this append or overwrite in juyptr?
        if not In_website:
            with open(save_to, "w") as my_file:
                my_file.write(self.insert_statement)
            if Partial_insert:
                self.make_smaller_insert_statements(save_to)
        return save_to

    def make_smaller_insert_statements(self, master_statement, perfile=1000):
        """
        Function makes smaller insert statements within text files for use in testing (only if In_website=False).
        Also returns list of smaller insert statements.
        :param master_statement: string containing master (large) insert_statement.
        :param perfile: number of rows to include per smaller text file (default 1000).
        :return: list of smaller insert statements
        """
        lines = []
        statements = []
        # Pull out header from master statement:
        header, master_statement = self.insert_statement.split("\n", maxsplit=1)
        lines = master_statement.split("\n")
        # Number of rows to insert:
        rows = len(lines)
        files_to_make = math.ceil(rows / perfile)
        file_num = 0
        for f in range(files_to_make):
            start = 0 + f * perfile
            end = (f + 1) * perfile
            # Create string containing smaller insert statement:
            if f == 0:
                temp = " ".join([header] + lines[start:end])
            else:
                temp = " ".join([header] + [" Values"] + lines[start:end])
            if temp[-1] == "\n":
                temp = temp[:-1]
            if temp[-1] == " ":
                temp = temp[:-1]
            if temp[-1] == ",":
                temp = temp[:-1] + ";"
            # Save smaller insert statement:
            statements.append(temp)
            if not In_website:
                filename = f"{self.study_name}_temp_insert_partial_{f}"
                with open(filename, "w") as my_file2:
                    my_file2.write(temp)
        return statements
        # TODO: make sure not missing last line

    def insert_header(self):
        """
        Creates header string for insert statement.
        :return: string containing header info for insert statement.
        """
        # Initialize string:
        # Column order of pandas dataframe, not database
        build_str = f"INSERT INTO {self.table_name} ("
        # Specify order variables will be specified in insert statement:
        for temp in self.col_names:
            build_str += f"{temp}, "
        # Remove last two values in string & finish building string:
        build_str = build_str[:-2]
        build_str += ")"
        # Return header string:
        return build_str


def what_is_this(input):
    # TODO put this back in class
    pass


# main function:
def main():
    """
    Main function to run to run entire program.
    """
    # More booleans to specify which part of code to run
    import_study1 = True  # Phase 1 sediment
    import_study2 = False  # UCR_2009_BeachSD # TODO: location ID key stopped working for combine
    import_study3 = False  # UCR_2010_BeachSD
    import_study4 = False  # UCR_2011_BeachSD
    import_study5 = False  # Phase 2 Sediment Teck Data
    import_study6 = True  # Bossburg
    import_study7 = False  # Phase 3 sediment
    import_study8 = False  # Phase 2 Sediment Trustee Data
    import_study9 = True  # Core Sample Results
    create_new_table = True  # Cannot be used when it website
    # Grab global variable:
    global Tunnel
    global Bioed_pw
    # Set up sshtunnel (for juyptr notebook):
    our_import = ImportTools()
    if In_jyptr:
        # connect to bioed via an ssh tunnel
        # do NOT include your password, use getpass
        Tunnel = sshtunnel.SSHTunnelForwarder(
            ('bioed.bu.edu', 22),
            ssh_username=Username,
            ssh_password=getpass.getpass(prompt='Password (bu): ', stream=None),
            remote_bind_address=('localhost', 4253))
        # the password requested here is your kerberos password that you use to access bioed
        # "activate" the ssh tunnel
        Tunnel.start()
        # Get the bioed password (once):
        Bioed_pw = getpass.getpass(prompt='Password (bioed): ', stream=None)
    # Create table:
    if create_new_table and not In_website:
        our_import.create_table()
    # Import study1:
    # Tunnel = None
    if import_study1:
        print("Importing study 1 (Phase 1 Sediment):")
        study1 = ImportStudy(the_input="Phase 1 Sediment.csv", study_name="Phase1Sediment", study_year=2005,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U ", utm_cord_system="Null", sep="|", is_csv=True)
        study1.run_import()
    if import_study2:
        print("Importing study 2 (UCR_2009_BeachSD):")
        study2 = ImportStudy(the_input="UCR_2009_BeachSD_fixed.xlsx", study_name="UCR_2009_BeachSD", study_year=2009,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study2.run_import()
        # TODO: handle "RinseBlank" in location table
    if import_study3:
        print("Importing study 3 (UCR_2010_BeachSD):")
        study3 = ImportStudy(the_input="UCR_2010_BeachSD_fixed.xlsx", study_name="UCR_2010_BeachSD", study_year=2010,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study3.run_import()
    if import_study4:
        print("Importing study 4 (UCR_2011_BeachSD):")
        study4 = ImportStudy(the_input="UCR_2011_BeachSD.xlsx", study_name="UCR_2011_BeachSD", study_year=2011,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study4.run_import()
    if import_study5:
        print("Importing study 5 (Phase 2 Sediment Teck Data):")
        study5 = ImportStudy(the_input="Phase 2 Sediment Teck Data.xls", study_name="Phase 2 Sediment Teck Data",
                             study_year=2013,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study5.run_import()
    if import_study6:
        print("Importing study 6 (Bossburg Data):")
        study6 = ImportStudy(the_input="Bossburg Data.xls", study_name="Bossburg", study_year=2015,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study6.run_import()
    if import_study7:
        print("Importing study 7 (Phase 3 Sediment):")
        study7 = ImportStudy(the_input="Phase 3 Sediment.xlsx", study_name="Phase 3 Sediment", study_year=2019,
                             sample_type="Sediment",
                             geo_cord_system="NAD83 UTM Zone 11U", utm_cord_system="NAD83 UTM Zone 11U", is_excel=True)
        study7.run_import()
    if import_study8:
        print("Importing study 8 (Phase 2 Sediment Trustee):")
        s8_files = {"chemistry": "phase2_sediment_trustee_chemistry_v2.csv",
                    "location and depth": "phase2_sediment_trustee_location_v2.csv"}
        s8_val = list(range(4, 26))
        s8_add = {"percent": list(range(4, 16)), "mg/kg": list(range(16, 26))}
        s8_merge = {"location and depth": ["Station", "Lab Sample ID", "Field ID"]}
        s8_col_expand = ["Analyte", "Units", "Value"]
        study8 = ImportStudy(the_input=s8_files,
                             study_name="Phase 2 Sediment Trustee Data",
                             study_year=2013, sample_type="Sediment",
                             geo_cord_system="Null", utm_cord_system="NAD83 UTM Zone 11U",
                             is_dict_filenames=True,
                             special_header=True,
                             special_col_names_expand=s8_col_expand,
                             special_cols_with_values=s8_val,
                             special_add_units_to_cols=s8_add,
                             special_merge_with=s8_merge)
        study8.run_import()
    if import_study9:
        print("Importing study 9 (Core Sample Results)")
        s9_file = "core_sample_results_data.csv"
        s9_val = list(range(7, 31))
        s9_add = {"mg/kg": list(range(7, 30)), "percent": [30]}
        s9_col_expand = ["Analyte", "Units", "Value"]
        study9 = ImportStudy(the_input=s9_file,
                             study_name="Core Sample Results",
                             study_year=2010, sample_type="Sediment",
                             geo_cord_system="WGS84_maybe", utm_cord_system="Null",
                             is_csv=True, special_header=True,
                             special_col_names_expand=s9_col_expand,
                             special_cols_with_values=s9_val,
                             special_add_units_to_cols=s9_add)
        study9.run_import()
        # TODO: convert to handle accepting strings
    # TODO core sample results MAY BE "WGS84"
    if In_jyptr:
        Tunnel.stop()


def test_code():
    """
    Test code for testing if implementation of dictionary of strings and dictionary of CSV works.
    """
    # Grab global variable:
    global Tunnel
    global Bioed_pw
    # Set up sshtunnel (for juyptr notebook):
    our_import = ImportTools()
    if In_jyptr:
        # connect to bioed via an ssh tunnel
        # do NOT include your password, use getpass
        Tunnel = sshtunnel.SSHTunnelForwarder(
            ('bioed.bu.edu', 22),
            ssh_username=Username,
            ssh_password=getpass.getpass(prompt='Password (bu): ', stream=None),
            remote_bind_address=('localhost', 4253))
        # the password requested here is your kerberos password that you use to access bioed
        # "activate" the ssh tunnel
        Tunnel.start()
        # Get the bioed password (once):
        Bioed_pw = getpass.getpass(prompt='Password (bioed): ', stream=None)
    # Testing string dicts:
    im_a_csv = "Phase 1 Sediment.csv"
    im_a_dict_of_files = {"lab_results": "phase3_labresults.csv", "locations": "phase3_location.csv"}
    dict_of_strings = {}
    with open("phase3_labresults.txt", "r", encoding='utf-8-sig') as my_file:
        my_string = my_file.read()
    dict_of_strings["labresults"] = my_string
    with open("phase3_location.txt", "r", encoding='utf-8-sig') as my_file:
        my_string = my_file.read()
    # TODO: drop down selecting labresult or locations
    dict_of_strings["locations"] = my_string
    print(my_string)  # Just to see what format the string is in
    string_study = ImportStudy(the_input=dict_of_strings, study_name="String Import4", study_year=9999,
                               sample_type="Sediment", geo_cord_system="Nonsense1", utm_cord_system="Nonsense3",
                               is_dict_strings=True)
    string_study.run_import()
    # Testing dict of CSV files:
    dict_of_csvs = {"lab_results": "phase3_labresults.csv", "locations": "phase3_location.csv"}
    csv_study = ImportStudy(the_input=dict_of_csvs, study_name="CSV Import4", study_year=9999, sample_type="Sediment",
                            geo_cord_system="Nonsense1", utm_cord_system="Nonsense2", is_dict_filenames=True)
    csv_study.run_import()
    # Re-test wacky csv:
    study1 = ImportStudy(the_input="Phase 1 Sediment.csv", study_name="WackyCSV4", study_year=9999,
                         sample_type="Sediment",
                         geo_cord_system="unknown_A1", utm_cord_system="unknown_A2", sep="|", is_csv=True)
    study1.run_import()
    if In_jyptr:
        Tunnel.stop()


if __name__ == '__main__':
    run_test_code = False
    main()
    if run_test_code:
        test_code()

# TODO indexes

# TODO: Phase 3 sediment: skip field measures
# TODO: do NOT skip field measurements for fish studies
# TODO: no blanks in the database
# TODO: check for columns with same name with different info and let mae rose know
# TODO: rename private
# TODO: handle data duplication
# TODO: don't need field measurements for sediment, will need for biological
# TODO: templates need to be able to handle deleted columns
# TODO: is column_names.txt handled reasonably?
# TODO: handle special headers for study 8 & 9 as strings
