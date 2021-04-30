#!/usr/local/Python-3.7/bin/python

# module load python

from data_import_charm import *

# Test with either Phase 1 Sediment (single text box) or
# phase 3 sediment (with lab_results text box & locations text box)

# To test on bioed without website:
dict_of_strings = {}
# Read in example strings (if not actually in website):
with open("phase3_labresults.txt", "r", encoding='utf-8-sig') as my_file:
    my_string = my_file.read()
dict_of_strings["labresults"] = my_string
with open("phase3_location.txt", "r", encoding='utf-8-sig') as my_file:
    my_string = my_file.read()
dict_of_strings["locations"] = my_string

# This is the example call:
string_study = ImportStudy(the_input=dict_of_strings, study_name="String FromBioed4", study_year=9999,
                           sample_type="Sediment", geo_cord_system="Nonsense1", utm_cord_system="Nonsense3",
                           is_dict_strings=True)
string_study.run_import()
