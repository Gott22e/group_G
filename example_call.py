# module load python

from data_import_charm import *

study1 = ImportStudy(the_file="Phase 1 Sediment.csv", study_name="Test1", study_year=2005,
                     sample_type="Sediment",
                     geo_cord_system="unknown_A1", utm_cord_system="unknown_A2")
study1.run_import()
