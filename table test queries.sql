-- Getting study counts
select study_name, count(*)
from cr
group by study_name

-- Get everything filtered as she would like:
select *
from cr 
where location_id != 'RinseBlank' and
lab_rep NOT IN ('2', '3', '4', 'BIO2', 'CONFRM', 'RE2') and 
material_analyzed not in ('RinseWater', 'Blank-Filteration', 'Porewater') and 
study_element != 'QC'

-- Testing insertion
select *
from cr_3
where study_name like "%Testing%"


select *, count(*)
from cr_3
where study_name like "%Testing%"
group by study_name

-- Getting Analyte names
select distinct analyte
from cr
order by analyte

-- to check sand, gravel, silt:
select distinct analyte
from cr
where analyte regexp 'sand|silt|gravel'

-- Getting Analyte names (and at least one study associated with it so you can see it)
select analyte, full_name, count(*), study_name
from cr
group by analyte
order by count(*) desc
-- can add:
select study_name, analyte, units, count(*)
from cr
where analyte like "%Carbon%" or analyte like "%TOC%" or analyte like "%carbon%"
group by analyte, study_name
order by count(*) desc

-- Figuring out where messed up names are coming from
select distinct analyte, study_name
from cr
order by analyte


-- Additional things to (not) add to string:
select *
from cr
where location_id != "RinseBlank"

-- Get lab replicates:
select DISTINCT lab_rep
from cr

-- Check that range is reasonable:
select study_name, max(upper_depth), max(lower_depth), max(depth_units), max(meas_value), max(river_mile), max(river_mile_dup), max(x_coord), max(y_coord), max(utm_x), max(utm_y), max(lat_WGS84_auto_calculated_only_for_mapping), max(lon_WGS84_auto_calculated_only_for_mapping), max(elevation), max(elevation_dup)
from cr
group by study_name

-- Phase 3 units:
SELECT * 
FROM `cr` 
WHERE study_name="Phase 3 Sediment" and location_id != 'RinseBlank' and
lab_rep NOT IN ('2', '3', '4', 'BIO2', 'CONFRM', 'RE2') and 
material_analyzed != 'RinseWater' and 
material_analyzed != 'Blank-Filteration'

-- Units from each study:
select study_name, analyte, units
from cr
where study_name = "Core Sample Results" or study_name = "Phase 2 Sediment Trustee Data"
group by study_name, analyte, units

-- Check split by flags:
select *
from cr
where study_name="Core Sample Results"

-- Units for sediment:
select study_name, depth_units
from cr
group by study_name, depth_units

-- Index play:
`alter` table cr add index if not exists analyte_idx (analyte); 
`alter` table cr add index if not exists date_idx (sample_date); 
alter table cr add index if not exists analyte_idx (analyte); 
alter table cr add index if not exists date_idx (sample_date); 
-- Create drop index statement:
`alter` table cr drop index if exists analyte_idx; 
alter table cr drop index if exists date_idx; 
-- TODO: if EXISTS!!!

-- Playing with graphics
select analyte, meas_value, river_mile
from cr_2
where river_mile is not Null and analyte = "Lead"
