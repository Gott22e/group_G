#!/usr/bin/env python3
"""
Program contains classes and functions to create data table and insert rows for the
Upper Columbia River Site database.

Program is designed to be run from within the juyptr notebook "data_import.ipynb".
(This is required for password management and tunneling.)

Updated: 2021/04/16

For issues with this script, contact Allison Nau.
"""
# Import packages
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
import sqlalchemy
# Requires xlrd, openpyxl for pandas excel support:
import xlrd
import openpyxl

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
# TODO test test
# TODO accept arguments passed from elsewhere?
# TODO error catching when insert statements don't work (including: when field is too long for database)

# Global variables:
Tunnel = None
Username = "anau"
Bioed_pw = None

# Booleans to specify what parts of the code to run:
# In_pycharm used to suppress functionality that is not currently enabled:
In_pycharm = False  # TODO fix
In_jyptr = True  # TODO fix
Import_study1 = True  # Phase 1 sediment
Import_study2 = True  # UCR_2009_BeachSD # TODO: location ID key stopped working for combine
Import_study3 = True  # UCR_2010_BeachSD
Import_study4 = True  # UCR_2011_BeachSD
Import_study5 = True  # Phase 2 Sediment Teck Data
Import_study6 = True  # Bossburg  # TODO: some of the rows aren't getting inserted, and this didn't break anything else
Import_study7 = True  # Phase 3 sediment  # TODO: this DID successfully insert
Create_new_table = True
Partial_insert = False  # TODO fix

# TODO: does Bossburg insert statements work?
# TODO: need to check that new create statement works
# TODO: test that all previous studies still get inserted properly
# TODO: do insert statement check before actually inserting

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
             ['location_id', 'principal_doc', 'river_mile', 'utm_x', 'utm_y', 'srid']]
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

    def __init__(self):
        """
        Initializes one ImportTools object.
        """
        # Variables we will add in:
        self.our_added_vars = ['study_name', 'study_year', 'sample_type', 'geo_cord_system', 'utm_cord_system']
        # Table name with bioed:
        self.table_name = "cr"
        # Study templates:
        self.known_templates = KnownStudyTemplates()
        # Variables to have in create table statement (in addition to our added variables):
        self.full_list = ['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg',
                          'anal_type', 'labsample', 'study_id', 'sample_no', 'sampcoll_id',
                          'sum_sample_id', 'sample_id', 'sample_date', 'study_element',
                          'composite_type', 'taxon', 'sample_material', 'sample_description',
                          'subsamp_type', 'upper_depth', 'lower_depth', 'depth_units',
                          'material_analyzed', 'method_code', 'meas_basis', 'lab_rep', 'analyte',
                          'full_name', 'cas_rn', 'original_lab_result', 'meas_value', 'units',
                          'sig_figs', 'lab_flags', 'qa_level', 'lab_conc_qual', 'validator_flags',
                          'detection_limit', 'reporting_limit', 'undetected', 'estimated',
                          'rejected', 'greater_than', 'tic', 'reportable', 'alias_id',
                          'nd_reported_to', 'qapp_deviation', 'nd_rationale', 'comments',
                          'river_mile', 'river_mile_dup', 'x_coord', 'y_coord', 'utm_x', 'utm_y', 'srid', 'srid_dup',
                          'lat_WGS84_auto_calculated_only_for_mapping', 'lon_WGS84_auto_calculated_only_for_mapping',
                          'principal_doc_location', 'elevation', 'elev_unit', 'elevation_dup', 'elev_ft']
        # Variable types:
        self.int_variables = ['sig_figs', 'detection_limit', 'reporting_limit']
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
        self.string_variables = ['study_loc_id', 'principal_doc', 'location_id', 'lab', 'lab_pkg', 'anal_type',
                                 'labsample', 'analyte', 'full_name', 'principal_doc_location',
                                 'study_id', 'sample_no', 'sampcoll_id', 'sum_sample_id', 'sample_id', "study_element",
                                 'composite_type', 'taxon', 'sample_material', 'subsamp_type',
                                 'depth_units', 'material_analyzed', 'method_code', 'meas_basis', 'units', 'lab_flags',
                                 'qa_level', 'lab_conc_qual', 'validator_flags',
                                 'undetected', 'estimated',
                                 'rejected', 'greater_than', 'tic', 'reportable',
                                 'alias_id', "cas_rn", 'lab_rep',
                                 'qapp_deviation', 'nd_reported_to', 'elev_unit']
        self.string_variables_long = ['comments', 'sample_description', 'nd_rationale']

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
            if In_jyptr:
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
        create_string += "study_name VARCHAR(200), \n"
        create_string += "study_year int, \n"
        create_string += "sample_type VARCHAR(200), \n"
        create_string += "geo_cord_system VARCHAR(100), \n"
        create_string += "utm_cord_system VARCHAR(100), \n"
        # Save file names as a text file:
        with open("column_names.txt", 'w') as my_file:
            for temp in self.our_added_vars:
                my_file.write(f"{temp}\n")
            for temp in self.full_list:
                my_file.write(f"{temp}\n")
        # Loop for variables to add (done this way to preserve order)
        for temp in self.full_list:
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
            for char in ["(", ")", ":", ";", " ", "-"]:
                new_col = new_col.replace(char, "_")
            for _ in range(5):
                new_col = new_col.replace("__", "_")
            if new_col in dict_of_swaps:
                new_col = dict_of_swaps[new_col]
            new_names.append(new_col)
        return new_names


class ImportStudy(ImportTools):
    """
    One object of this class contains variables and methods required to import data from a study into the database.
    Inherits from ImportTools.
    """

    def __init__(self, the_file, study_name, study_year, sample_type, geo_cord_system, utm_cord_system,
                 is_csv=True, special_header=False):
        """
        Initializes one ImportStudy object.
        :param the_file: csv or excel file containing study data.
        :param study_name: string containing name to be given to the study within the database.
        :param study_year: integer representing study year.
        :param sample_type: string containing study type.
        :param geo_cord_system: string specifying coordinate system used in x & y coordinate columns.
        :param utm_cord_system: string specifying coordinate system used in x & y utm coordinate columns.
        :param is_csv: boolean specifying if input file is a csv (True, default) or an excel file.
        :param special_header: boolean specifying if there is formatting in header of excel file that
        must be adhered to.
        """
        # Initialize variables from parent:
        super().__init__()
        # Initialize variables:
        self.the_file = the_file
        self.is_csv = is_csv
        self.study_name = study_name
        self.study_year = study_year
        self.sample_type = sample_type
        self.geo_cord_system = geo_cord_system
        self.utm_cord_system = utm_cord_system
        self.special_header = special_header
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
        self.table = self.read_in_study(filename=self.the_file)
        # Compare column headers with other studies to see what templates make sense:
        self.compare_with_other_studies()
        if self.found_template:  # If template was found, proceed
            # If study was an excel file, combine into one table, according to template in self.use_template
            if not self.is_csv:
                self.combine_sheets()
            # If template was found, go ahead and add columns with the study info:
            if self.use_template is not None:
                self.finish_building_table()  # Finish building table, including added columns with study info
        # Compare columns of current study to master database table and references:
        self.check_columns()

    def read_in_study(self, filename):
        """
        Read in data stored in "filename".
        :param filename: excel or csv file containing data.
        :return: dataframe (csv) or list of dataframes (excel) containing study data.
        """
        if self.is_csv:  # TODO: Template 0?
            table = ImportTools.read_in_csv(filename)
            table.columns = self.clean_col_names(table)  # TODO: does this work for csv?
            self.col_names_by_sheet["sheet1"] = table.columns
        else:
            table = ImportTools.read_in_excel(filename, self.sample_type)
            for t in table:
                table[t].columns = self.clean_col_names(table[t])
                self.col_names_by_sheet[t] = table[t].columns
        return table

    def combine_sheets(self):
        """
        Combines sheets that were stored in study's excel data file, according to template.
        This should not be used if template is unknown.
        """
        template = self.use_template
        temp_table = None
        print(f"Template to use: {template}")
        if template == 1 or template == 2 or template == 3:
            temp_table = self.template1_clean()
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
        Cleans up studies that follow template1, and combines into one dataframe.
        :return: one cleaned up pandas dataframe.
        # TODO update documentation regarding template 2 & 3
        """
        # TODO: need to confirm this is OK renamed column for all studies of this template
        # Rename columms that are duplicated on different sheets, but are not being used as part of the join:
        # TODO: manage these sheet names better! (maybe csv files should have drop down)
        if "labresults" in self.table:
            self.table["labresult"] = self.table.pop("labresults")
        if "lab results" in self.table:
            self.table["labresult"] = self.table.pop("lab results")
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
            group_by_cols.remove("principal_doc_location")
            self.table["locations"] = self.table["locations"].groupby(group_by_cols)['principal_doc_location'].apply(
                ', '.join).reset_index()
        print(self.table["locations"].head(n=5))
        # Merge sheets:
        temp_table = pd.merge(self.table['labresult'], self.table['locations'], on=["location_id"], how="left")
        temp_table = self.clean_numeric_cols_of_nulls(temp_table)
        return temp_table

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
        # TODO: handle filling in missing columns? -> probably not necessary
        print("Column names after table built:")
        print(self.table.columns)

    def run_import(self):
        """
        Runs import of current data table.
        """
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
                if In_jyptr:
                    if len(self.table.index) <= 5000:
                        self.execute_query(self.insert_statement)
                    else:
                        smaller_statements = self.make_smaller_insert_statements(master_statement=master_statement,
                                                                                 perfile=2000)
                        for query in smaller_statements:
                            self.execute_query(query=query)
            # TODO: modify to raise error?
            # Save current known templates:
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
                    # TODO: test!!!!!!!
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
        with open(save_to, "w") as my_file:
            my_file.write(self.insert_statement)
        if Partial_insert:
            self.make_smaller_insert_statements(save_to)
        return save_to

    def make_smaller_insert_statements(self, master_statement, perfile=1000):
        """
        Function makes smaller insert statements within text files for use in testing.
        :param master_statement: text file containing master (large) insert_statement.
        :param perfile: number of rows to include per smaller text file.
        :return: list of smaller insert statements
        """
        lines = []
        statements = []
        # Read in master inser statement file:
        with open(master_statement, "r") as my_file:
            header = my_file.readline()
            for line in my_file:
                lines.append(line)
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


# main function:
def main():
    """
    Main function to run to run entire program.
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
    # Create table:
    if Create_new_table:
        our_import.create_table()
    # Import study1:
    # Tunnel = None
    if Import_study1:
        print("Importing study 1 (Phase 1 Sediment):")
        study1 = ImportStudy(the_file="Phase 1 Sediment.csv", study_name="Phase1Sediment", study_year=2005,
                             sample_type="Sediment",
                             geo_cord_system="unknown_A1", utm_cord_system="unknown_A2")
        study1.run_import()
    if Import_study2:
        print("Importing study 2 (UCR_2009_BeachSD):")
        study2 = ImportStudy(the_file="UCR_2009_BeachSD.xlsx", study_name="UCR_2009_BeachSD", study_year=2009,
                             sample_type="Sediment",
                             geo_cord_system="unknown_B1", utm_cord_system="unknown_B2", is_csv=False)
        study2.run_import()
        # TODO: handle "RinseBlank" in location table
    if Import_study3:
        print("Importing study 3 (UCR_2010_BeachSD):")
        study3 = ImportStudy(the_file="UCR_2010_BeachSD.xlsx", study_name="UCR_2010_BeachSD", study_year=2010,
                             sample_type="Sediment",
                             geo_cord_system="unknown_C1", utm_cord_system="unknown_C2", is_csv=False)
        study3.run_import()
    if Import_study4:
        print("Importing study 4 (UCR_2011_BeachSD):")
        study4 = ImportStudy(the_file="UCR_2011_BeachSD.xlsx", study_name="UCR_2011_BeachSD", study_year=2011,
                             sample_type="Sediment",
                             geo_cord_system="unknown_D1", utm_cord_system="unknown_D2", is_csv=False)
        study4.run_import()
    if Import_study5:
        print("Importing study 5 (Phase 2 Sediment Teck Data):")
        study5 = ImportStudy(the_file="Phase 2 Sediment Teck Data.xls", study_name="Phase 2 Sediment Teck Data",
                             study_year=2013,
                             sample_type="Sediment",
                             geo_cord_system="unknown_E1", utm_cord_system="unknown_E2", is_csv=False)
        study5.run_import()
    if Import_study6:
        print("Importing study 6 (Bossburg Data):")
        study6 = ImportStudy(the_file="Bossburg Data.xls", study_name="Bossburg", study_year=2015,
                             sample_type="Sediment",
                             geo_cord_system="unknown_F1", utm_cord_system="unknown_F2", is_csv=False)
        study6.run_import()
    if Import_study7:
        print("Importing study 7 (Phase 3 Sediment):")
        study7 = ImportStudy(the_file="Phase 3 Sediment.xlsx", study_name="Phase 3 Sediment", study_year=2019,
                             sample_type="Sediment",
                             geo_cord_system="unknown_G1", utm_cord_system="unknown_G2", is_csv=False)
        study7.run_import()
    if In_jyptr:
        Tunnel.stop()


if __name__ == '__main__':
    main()

# TODO indexes

# TODO: Phase 3 sediment: skip field measures
# TODO: do NOT skip field measurements for fish studies
# TODO: no blanks in the database
# TODO: check for columns with same name with different info and let mae rose know
# TODO: rename private
# TODO: handle data duplication
# TODO: don't need field measurements for sediment, will need for biological
# TODO: templates need to be able to handle deleted columns
