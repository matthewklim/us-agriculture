-- the bulk of the time for this script will be importing data
-- finishes ~7 minutes on a warehouse sized to x-small
-- using snowflake sql
-- create a table to import data
-- take column names from file
-- rename cv_% to cv_percent for sql friendly name
-- test with sample
-- tab delimited separator 
-- lets bring everything in without fighting data type
-- cluster table by our likely heavily used dimensions - year and state
-- make any modifications and data structure changes
-- export cleaned result
-- this version of the file contains limited comments

/* create an empty table */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp (
       SOURCE_DESC VARCHAR NULL,
       SECTOR_DESC VARCHAR NULL,
       GROUP_DESC VARCHAR NULL,
       COMMODITY_DESC VARCHAR NULL,
       CLASS_DESC VARCHAR NULL,
       PRODN_PRACTICE_DESC VARCHAR NULL,
       UTIL_PRACTICE_DESC VARCHAR NULL,
       STATISTICCAT_DESC VARCHAR NULL,
       UNIT_DESC VARCHAR NULL,
       SHORT_DESC VARCHAR NULL,
       DOMAIN_DESC VARCHAR NULL,
       DOMAINCAT_DESC VARCHAR NULL,
       AGG_LEVEL_DESC VARCHAR NULL,
       STATE_ANSI VARCHAR NULL,
       STATE_FIPS_CODE VARCHAR NULL,
       STATE_ALPHA VARCHAR NULL,
       STATE_NAME VARCHAR NULL,
       ASD_CODE VARCHAR NULL,
       ASD_DESC VARCHAR NULL,
       COUNTY_ANSI VARCHAR NULL,
       COUNTY_CODE VARCHAR NULL,
       COUNTY_NAME VARCHAR NULL,
       REGION_DESC VARCHAR NULL,
       ZIP_5 VARCHAR NULL,
       WATERSHED_CODE VARCHAR NULL,
       WATERSHED_DESC VARCHAR NULL,
       CONGR_DISTRICT_CODE VARCHAR NULL,
       COUNTRY_CODE VARCHAR NULL,
       COUNTRY_NAME VARCHAR NULL,
       LOCATION_DESC VARCHAR NULL,
       YEAR INT NOT NULL,
       FREQ_DESC VARCHAR NULL,
       BEGIN_CODE VARCHAR NULL,
       END_CODE VARCHAR NULL,
       REFERENCE_PERIOD_DESC VARCHAR NULL,
       WEEK_ENDING VARCHAR NULL,
       LOAD_TIME VARCHAR NULL,
       VALUE VARCHAR NULL,
       CV_PERCENT VARCHAR NULL
) CLUSTER BY (YEAR,STATE_NAME) ;


/* Test load and clear */
PUT file://c:\users\matth\downloads\qs.sample.txt @analyst_data.%ag_exp 
parallel = 99
auto_compress = TRUE
overwrite = TRUE ;


COPY INTO analyst_data.ag_exp
FROM @analyst_data.%ag_exp
file_format = (
type = 'CSV'
field_delimiter = '\t'
skip_header = 1
ESCAPE = '\\'
trim_space = TRUE
empty_field_as_null = TRUE
) ;

RM @analyst_data.%ag_exp; 

/*
* bring the downloaded raw data into snowflake
* use the the max number of threads available in snowflake for parallel processing speed
* let snowflake compress the file and overwrite existing files if needed to rerun or rebuild
* utilize the stage created with the table for storing the import file
*/


PUT file://c:\users\matth\downloads\qs.crops_20200506.txt.gz @analyst_data.%ag_exp 
parallel = 99
auto_compress = TRUE
source_compression = gzip
overwrite = TRUE ;

/* load the data into the newly created empty table */
TRUNCATE TABLE analyst_data.ag_exp ;
COPY INTO analyst_data.ag_exp
FROM @analyst_data.%ag_exp
file_format = (
type = 'CSV'
compression = 'gzip'
field_delimiter = '\t'
skip_header = 1
escape = '\\'
trim_space = TRUE
null_if = ('NULL','null','\\N', '')
empty_field_as_null = TRUE
validate_utf8 = TRUE
encoding = 'UTF8'
) ;

/*
* no notable issues importing the data 
* data set seems clear of odd invalid characters 
* row checks does not show obvious signs of any data dropped during import
* did not see signs of separators contained in the file that would cause data to shift across columns 
*/

/* restrict output to desired commodities after 1990 */ 
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_core LIKE analyst_data.ag_exp;
INSERT INTO analyst_data.ag_exp_core
SELECT * FROM analyst_data.ag_exp
WHERE year >= 1990
AND REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie')
AND NOT commodity_desc = 'SWEET CORN' ;


/* Oddly the dataset only contains information for different rice varieties at the state and notional level 
* Leave rice aggregated to all classes for comparing totals to the state and national level
* Corn also needs to be treated differently
       * Grain vs silage are not technically considered varieties, but acreage still rolls up into Corn
       * Corn typically measured in bushels
* Conversion reference - https://www.ers.usda.gov/webdocs/publications/41880/33132_ah697_002.pdf?v=0       
* Using the usda conversion table - can bring roughly standardize and convert 1 us short ton to 56 lb bushels 
* (2000lb/ton / 56lb/bushel = 35.714 bushels per ton)
* $ amounts not recorded at the county level - omitting from cleaned county dataset
*/

/* cleanup for known counties */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_known_county AS 
WITH crops_with_variety AS (
SELECT
       commodity_desc
,      'ALL CLASSES'                                                                       AS class_desc       
FROM
       analyst_data.ag_exp_core
WHERE
       commodity_desc <> 'CORN'
AND
       agg_level_desc = 'COUNTY'       
GROUP BY
       1
HAVING 
       COUNT(DISTINCT class_desc) > 1
       )
SELECT 
       year
,      TO_DATE(year||'-01-01')                                                             AS calendar_date
,      state_fips_code||county_code                                                        AS fips_county_code
,      state_alpha
,      state_name
,      county_name
,      commodity_desc
,      INITCAP(commodity_desc)                                                             AS commodity_name
,      CASE
              WHEN commodity_desc = 'CORN' AND statisticcat_desc <> 'AREA PLANTED' 
              THEN util_practice_desc
              WHEN commodity_desc = 'CORN' AND statisticcat_desc = 'AREA PLANTED' 
              THEN 'AREA PLANTED (ONLY)'
              ELSE class_desc
       END                                                                                 AS commodity_class_desc
,      INITCAP(commodity_class_desc)                                                       AS commodity_class_variety
,      class_desc
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU'
                     WHEN commodity_desc = 'COTTON'                  
                            THEN '480 LB BALES'
                     WHEN commodity_desc = 'RICE'
                            THEN 'CWT'
              END
              )                                                                            AS production_unit
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS TO BU'
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 'BU TO TONS'                            
                     ELSE 'NO CONVERSION'
              END
              )                                                                            AS production_unit_conversion
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 35.714
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 0.028                            
                     ELSE 1
              END
              )                                                                            AS production_unit_multiplier
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS / ACRE'              
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU / ACRE'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')                
                            THEN 'LB / ACRE'
              END
              )                                                                            AS yield_unit
,      ANY_VALUE(
              CASE    
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 1
                     WHEN commodity_desc = 'COTTON'
                            THEN 480                            
                     WHEN commodity_desc = 'RICE'
                            THEN 100
              END
              )::FLOAT                                                                     AS production_to_yield_multiplier
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA PLANTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_planted
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA HARVESTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_harvested
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'TONS'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)                            
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'BU'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = '480 LB BALES'
                     AND commodity_desc = 'COTTON'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'CWT'
                     AND commodity_desc = 'RICE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_production
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'TONS / ACRE'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'BU / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'LB / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_yield
FROM
       analyst_data.ag_exp_core
WHERE
       agg_level_desc = 'COUNTY'
AND
       source_desc = 'SURVEY'       
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
AND
       prodn_practice_desc = 'ALL PRODUCTION PRACTICES'       
AND 
       NOT EXISTS (
       SELECT TRUE
       FROM crops_with_variety
       WHERE ag_exp_core.commodity_desc = crops_with_variety.commodity_desc
       AND ag_exp_core.class_desc = crops_with_variety.class_desc
       )            
GROUP BY 
       1,2,3,4,5,6,7,8,9,10,11 ;

/* cleanup for county comparison to state and use for backfill unspecified county data within states */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_clean_state AS 
WITH crops_with_variety AS (
SELECT
       commodity_desc
,      class_desc
FROM
       analyst_data.ag_exp_known_county
GROUP BY
       1,2              
       )
SELECT 
       year
,      TO_DATE(year||'-01-01')                                                             AS calendar_date
,      state_fips_code||county_code                                                        AS fips_county_code
,      state_alpha
,      state_name
,      county_name
,      commodity_desc
,      INITCAP(commodity_desc)                                                             AS commodity_name
,      CASE
              WHEN commodity_desc = 'CORN' AND statisticcat_desc <> 'AREA PLANTED' 
              THEN util_practice_desc
              WHEN commodity_desc = 'CORN' AND statisticcat_desc = 'AREA PLANTED' 
              THEN 'AREA PLANTED (ONLY)'
              ELSE class_desc
       END                                                                                 AS commodity_class_desc
,      INITCAP(commodity_class_desc)                                                       AS commodity_class_variety
,      class_desc
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU'
                     WHEN commodity_desc = 'COTTON'                  
                            THEN '480 LB BALES'
                     WHEN commodity_desc = 'RICE'
                            THEN 'CWT'
              END
              )                                                                            AS production_unit
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS TO BU'
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 'BU TO TONS'                            
                     ELSE 'NO CONVERSION'
              END
              )                                                                            AS production_unit_conversion
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 35.714
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 0.028                            
                     ELSE 1
              END
              )                                                                            AS production_unit_multiplier
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS / ACRE'              
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU / ACRE'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')                
                            THEN 'LB / ACRE'
              END
              )                                                                            AS yield_unit
,      ANY_VALUE(
              CASE    
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 1
                     WHEN commodity_desc = 'COTTON'
                            THEN 480                            
                     WHEN commodity_desc = 'RICE'
                            THEN 100
              END
              )::FLOAT                                                                     AS production_to_yield_multiplier
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA PLANTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_planted
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA HARVESTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_harvested
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'TONS'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)                            
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'BU'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = '480 LB BALES'
                     AND commodity_desc = 'COTTON'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'CWT'
                     AND commodity_desc = 'RICE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_production
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'TONS / ACRE'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'BU / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'LB / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_yield
FROM
       analyst_data.ag_exp_core
WHERE
       agg_level_desc = 'STATE'
AND
       source_desc = 'SURVEY'       
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
AND
       prodn_practice_desc = 'ALL PRODUCTION PRACTICES'
AND EXISTS (
       SELECT TRUE
       FROM crops_with_variety
       WHERE ag_exp_core.commodity_desc = crops_with_variety.commodity_desc
       AND ag_exp_core.class_desc = crops_with_variety.class_desc
       )       
GROUP BY 
       1,2,3,4,5,6,7,8,9,10,11 ;

/* cleanup for comparison to national comparison to state */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_clean_national AS 
WITH crops_with_variety AS (
SELECT
       commodity_desc
,      class_desc
FROM
       analyst_data.ag_exp_known_county
GROUP BY
       1,2              
       )
SELECT 
       year
,      TO_DATE(year||'-01-01')                                                             AS calendar_date
,      state_fips_code||county_code                                                        AS fips_county_code
,      state_alpha
,      state_name
,      county_name
,      commodity_desc
,      INITCAP(commodity_desc)                                                             AS commodity_name
,      CASE
              WHEN commodity_desc = 'CORN' AND statisticcat_desc <> 'AREA PLANTED' 
              THEN util_practice_desc
              WHEN commodity_desc = 'CORN' AND statisticcat_desc = 'AREA PLANTED' 
              THEN 'AREA PLANTED (ONLY)'
              ELSE class_desc
       END                                                                                 AS commodity_class_desc
,      INITCAP(commodity_class_desc)                                                       AS commodity_class_variety
,      class_desc
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU'
                     WHEN commodity_desc = 'COTTON'                  
                            THEN '480 LB BALES'
                     WHEN commodity_desc = 'RICE'
                            THEN 'CWT'
              END
              )                                                                            AS production_unit
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS TO BU'
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 'BU TO TONS'                            
                     ELSE 'NO CONVERSION'
              END
              )                                                                            AS production_unit_conversion
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 35.714
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'GRAIN'
                            THEN 0.028                            
                     ELSE 1
              END
              )                                                                            AS production_unit_multiplier
,      ANY_VALUE(
              CASE
                     WHEN commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN 'TONS / ACRE'              
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 'BU / ACRE'
                     WHEN REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')                
                            THEN 'LB / ACRE'
              END
              )                                                                            AS yield_unit
,      ANY_VALUE(
              CASE    
                     WHEN REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN 1
                     WHEN commodity_desc = 'COTTON'
                            THEN 480                            
                     WHEN commodity_desc = 'RICE'
                            THEN 100
              END
              )::FLOAT                                                                     AS production_to_yield_multiplier
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA PLANTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_planted
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'AREA HARVESTED'
                     AND unit_desc = 'ACRES'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS area_harvested
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'TONS'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)                            
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'BU'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*','ie')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = '480 LB BALES'
                     AND commodity_desc = 'COTTON'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'PRODUCTION'
                     AND unit_desc = 'CWT'
                     AND commodity_desc = 'RICE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_production
,      SUM(
              CASE 
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'TONS / ACRE'
                     AND commodity_desc = 'CORN'
                     AND commodity_class_desc = 'SILAGE'
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'BU / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(CORN|SOYBEANS|WHEAT).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
                     WHEN statisticcat_desc = 'YIELD'
                     AND unit_desc = 'LB / ACRE'
                     AND REGEXP_LIKE(commodity_desc,'.*(COTTON|RICE).*')
                            THEN TRY_TO_NUMBER(value,'999,999,999,999.999',38,3)
              END                     
       )                                                                                   AS crop_yield
FROM
       analyst_data.ag_exp_core
WHERE
       agg_level_desc = 'NATIONAL'
AND
       source_desc = 'SURVEY'       
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
AND
       prodn_practice_desc = 'ALL PRODUCTION PRACTICES'
AND EXISTS (
       SELECT TRUE
       FROM crops_with_variety
       WHERE ag_exp_core.commodity_desc = crops_with_variety.commodity_desc
       AND ag_exp_core.class_desc = crops_with_variety.class_desc
       )       
GROUP BY 
       1,2,3,4,5,6,7,8,9,10,11 ;

/* Roll up unclassified data into unspecified counties for backfill and enable aggregation to national totals
* Should not have any missing state data going from counties to state 
* We see some odd totals where counties have higher volume - but it happens extremely rarely and stops after 2013 (11x total)
*/ 

/* calculate differences missing in state aggregates between county and state level tables */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_unspecified_counties AS
WITH known_county AS (
SELECT
       year
,      commodity_desc
,      commodity_class_desc
,      state_name
,      SUM(area_planted)                                                                   AS area_planted
,      SUM(area_harvested)                                                                 AS area_harvested
,      SUM(crop_production)                                                                AS crop_production
,      SUM(crop_yield)                                                                     AS crop_yield
FROM
       analyst_data.ag_exp_known_county
GROUP BY
       1,2,3,4
       )
SELECT
       ag_exp_clean_state.year
,      ag_exp_clean_state.commodity_desc
,      ag_exp_clean_state.commodity_class_desc
,      ag_exp_clean_state.state_alpha
,      ag_exp_clean_state.state_name
,      'UNSPECIFIED'                                                                       AS county_name
,      known_county.state_name                                                             AS county_state_name
,      production_unit
,      production_unit_conversion
,      production_unit_multiplier
,      yield_unit
,      production_to_yield_multiplier
,      NULLIF(ag_exp_clean_state.area_planted - COALESCE(known_county.area_planted,0),0)   AS area_planted
,      NULLIF(
              ag_exp_clean_state.area_harvested - COALESCE(known_county.area_harvested,0)
              ,0)                                                                          AS area_harvested
,      NULLIF(
              ag_exp_clean_state.crop_production - COALESCE(known_county.crop_production,0)
              ,0)                                                                          AS crop_production
FROM
       analyst_data.ag_exp_clean_state
FULL JOIN
       known_county 
ON
       ag_exp_clean_state.year = known_county.year
AND
       ag_exp_clean_state.commodity_desc = known_county.commodity_desc
AND      
       ag_exp_clean_state.commodity_class_desc = known_county.commodity_class_desc
AND
       ag_exp_clean_state.state_name = known_county.state_name ;


/* create a fully cleaned up dataset with backfilled unspecified counties and human friendly readable names for final output */
CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_clean_county 
CLUSTER BY (YEAR,fips_county_code) AS
SELECT 
       year
,      calendar_date
,      fips_county_code
,      state_alpha
,      INITCAP(state_name)                                                                 AS state_name
,      INITCAP(county_name)                                                                AS county_name
,      commodity_name
,      commodity_class_variety
,      production_unit
,      production_unit_conversion
,      production_unit_multiplier
,      yield_unit
,      production_to_yield_multiplier
,      area_planted
,      area_harvested
,      crop_production
,      crop_yield   
FROM
       analyst_data.ag_exp_known_county
UNION ALL
SELECT
       year
,      TO_DATE(year||'-01-01')                                                             AS calendar_date
,      NULL                                                                                AS fips_county_code
,      state_alpha
,      INITCAP(state_name)                                                                 AS state_name
,      INITCAP(county_name)                                                                AS county_name
,      INITCAP(commodity_desc)                                                             AS commodity_name
,      INITCAP(commodity_class_desc)                                                       AS commodity_class_variety
,      production_unit
,      production_unit_conversion
,      production_unit_multiplier
,      yield_unit
,      production_to_yield_multiplier
,      area_planted
,      area_harvested
,      crop_production
,      NULL                                                                                AS crop_yield
FROM 
       analyst_data.ag_exp_unspecified_counties
WHERE 
       NOT area_planted IS NULL
OR
       NOT area_harvested IS NULL 
OR 
       NOT crop_production IS NULL ;

/* export cleaned dataset as a text file using the same tab delimiter as the original data file */
COPY INTO @analyst_data.%ag_exp_clean_county/ag_exp_clean_county.txt.gz
FROM analyst_data.ag_exp_clean_county
FILE_FORMAT = (
        type = CSV
        field_delimiter = '\t'
        escape = '\\'
        null_if = ('','NULL','null','\\N')
        empty_field_as_null = FALSE
        compression = gzip
        file_extension = 'gz'
    )
HEADER = TRUE
overwrite = TRUE 
single = TRUE ;

/* Can check payload */ 
LIST @analyst_data.%ag_exp_clean_county ;

/* Save to local directory */
GET @analyst_data.%ag_exp_clean_county/ag_exp_clean_county file://C:\Users\matth\downloads ;