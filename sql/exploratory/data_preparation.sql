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
-- this version contains scratch work

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

/*
* load the data into the newly created empty table
*/
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


--- could start separating these into separate files, but have one file for to simplifying following from the top down
--- can also link to sections in the README.md doc

/*
Sanity Check the data import
*/

/* see if first and last row made it into the table.  */ 
SELECT 
       *
FROM 
       analyst_data.ag_exp 
WHERE 
       class_desc  IN ('INDOOR USE, HYDRANGEA','WAREHOUSE, GENERAL')
AND
       util_practice_desc IN ('RETAIL, POTS','ALL UTILIZATION PRACTICES')
AND 
       unit_desc IN ('POTS','NUMBER')
AND
       state_fips_code IN ('01','99')
AND
       state_alpha IN ('AL','US')
AND
       YEAR IN (2009,2019)
AND
       value IN ('820','257') ;

/*
SOURCE_DESC|SECTOR_DESC|GROUP_DESC  |COMMODITY_DESC          |CLASS_DESC           |PRODN_PRACTICE_DESC     |UTIL_PRACTICE_DESC       |STATISTICCAT_DESC|UNIT_DESC|SHORT_DESC                                                                             |DOMAIN_DESC          |DOMAINCAT_DESC                                  |AGG_LEVEL_DESC|STATE_ANSI|STATE_FIPS_CODE|STATE_ALPHA|STATE_NAME|ASD_CODE|ASD_DESC|COUNTY_ANSI|COUNTY_CODE|COUNTY_NAME|REGION_DESC|ZIP_5|WATERSHED_CODE|WATERSHED_DESC|CONGR_DISTRICT_CODE|COUNTRY_CODE|COUNTRY_NAME |LOCATION_DESC|YEAR|FREQ_DESC    |BEGIN_CODE|END_CODE|REFERENCE_PERIOD_DESC|WEEK_ENDING|LOAD_TIME          |VALUE|CV_PERCENT|
-----------|-----------|------------|------------------------|---------------------|------------------------|-------------------------|-----------------|---------|---------------------------------------------------------------------------------------|---------------------|------------------------------------------------|--------------|----------|---------------|-----------|----------|--------|--------|-----------|-----------|-----------|-----------|-----|--------------|--------------|-------------------|------------|-------------|-------------|----|-------------|----------|--------|---------------------|-----------|-------------------|-----|----------|
CENSUS     |CROPS      |HORTICULTURE|FLOWERING PLANTS, POTTED|INDOOR USE, HYDRANGEA|ALL PRODUCTION PRACTICES|RETAIL, POTS             |SALES            |POTS     |FLOWERING PLANTS, POTTED, INDOOR USE, HYDRANGEA, RETAIL, POTS - SALES, MEASURED IN POTS|TOTAL                |NOT SPECIFIED                                   |STATE         |01        |01             |AL         |ALABAMA   |        |        |           |           |           |           |     |00000000      |              |                   |9000        |UNITED STATES|ALABAMA      |2009|ANNUAL       |00        |00      |YEAR                 |           |2015-01-31 00:00:00|820  |          |
SURVEY     |CROPS      |COMMODITIES |COLD STORAGE CAPACITY   |WAREHOUSE, GENERAL   |ALL PRODUCTION PRACTICES|ALL UTILIZATION PRACTICES|WAREHOUSES       |NUMBER   |COLD STORAGE CAPACITY, WAREHOUSE, GENERAL - NUMBER OF WAREHOUSES                       |REFRIGERATED CAPACITY|REFRIGERATED CAPACITY: (5,000,000 OR MORE CU FT)|NATIONAL      |          |99             |US         |US TOTAL  |        |        |           |           |           |           |     |00000000      |              |                   |9000        |UNITED STATES|US TOTAL     |2019|POINT IN TIME|10        |10      |FIRST OF OCT         |           |2020-03-30 11:58:11|257  |          |
*/

/* Row Count Check
* A couple of easy ways we can check:
* PowerShell - can count csv file
* open in text editor that can view large files (e.g. Large text viewer)
-- looking for 19,157,951 rows (not including the header row)
 */ 

SELECT
       SUM(1)                                                                              AS row_count
FROM
       analyst_data.ag_exp ;

/*
The row count in our Snowflake table also contains 19,157,951 rows
Building towards a cleaned up dataaset
 * We can start by filtering out data before 1990
 * And limiting the dataset to five types of crops: corn, soy, wheat, cotton, and rice
Lets take a look at what some of the combinations of data look like 
*/

SELECT 
       freq_desc
,      reference_period_desc       
,      SUM(1)                                                                              AS row_count
FROM 
       analyst_data.ag_exp
WHERE
       year >= 1990
AND
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
GROUP BY
       freq_desc
,      reference_period_desc ;

/* Interesting the dataset comes with multiple levels of reporting periods
* For this exercise we can focus on annual frequency & year period reporting 
FREQ_DESC    |REFERENCE_PERIOD_DESC|ROW_COUNT|
-------------|---------------------|---------|
ANNUAL       |YEAR - JUL FORECAST  |     2974|
ANNUAL       |YEAR - OCT ACREAGE   |      160|
ANNUAL       |YEAR - AUG ACREAGE   |      121|
ANNUAL       |YEAR - MAY FORECAST  |     1603|
ANNUAL       |YEAR - DEC FORECAST  |     2054|
ANNUAL       |MARKETING YEAR       |     8389|
ANNUAL       |YEAR - JUN FORECAST  |     1355|
ANNUAL       |YEAR - SEP ACREAGE   |      130|
ANNUAL       |YEAR - JAN FORECAST  |      223|
ANNUAL       |YEAR                 |  2931127|
ANNUAL       |YEAR - OCT FORECAST  |     5475|
ANNUAL       |YEAR - SEP FORECAST  |     6287|
ANNUAL       |YEAR - AUG FORECAST  |     7521|
ANNUAL       |YEAR - JAN ACREAGE   |       41|
ANNUAL       |YEAR - NOV FORECAST  |     7398|
ANNUAL       |YEAR - JUN ACREAGE   |     2271|
ANNUAL       |YEAR - DEC ACREAGE   |      276|
ANNUAL       |YEAR - MAR ACREAGE   |     2124|
MONTHLY      |FEB                  |     5411|
MONTHLY      |APR THRU JUN         |      112|
MONTHLY      |JUN THRU AUG         |        8|
MONTHLY      |NOV                  |     5901|
MONTHLY      |JUL                  |     5429|
MONTHLY      |MAR                  |     5770|
MONTHLY      |JAN                  |     5700|
MONTHLY      |OCT THRU DEC         |      135|
MONTHLY      |JUL THRU SEP         |      135|
MONTHLY      |SEP THRU NOV         |       40|
MONTHLY      |DEC THRU FEB         |       41|
MONTHLY      |MAY                  |    14331|
MONTHLY      |OCT                  |     5933|
MONTHLY      |AUG                  |     5869|
MONTHLY      |DEC                  |     8034|
MONTHLY      |APR                  |     5127|
MONTHLY      |JAN THRU MAR         |      134|
MONTHLY      |SEP                  |     6164|
MONTHLY      |JUN                  |     5789|
POINT IN TIME|END OF JUN           |       60|
POINT IN TIME|FIRST OF OCT         |     1380|
POINT IN TIME|MID NOV              |     2292|
POINT IN TIME|FIRST OF MAR         |    12977|
POINT IN TIME|MID DEC              |     2444|
POINT IN TIME|FIRST OF NOV         |     1835|
POINT IN TIME|END OF JAN           |       62|
POINT IN TIME|END OF FEB           |       62|
POINT IN TIME|FIRST OF DEC         |    12676|
POINT IN TIME|FIRST OF SEP         |     8972|
POINT IN TIME|END OF SEP           |       60|
POINT IN TIME|MID OCT              |     1871|
POINT IN TIME|FIRST OF FEB         |     2414|
POINT IN TIME|FIRST OF JUL         |        9|
POINT IN TIME|END OF APR           |       60|
POINT IN TIME|FIRST OF JUN         |     9497|
POINT IN TIME|END OF MAY           |       60|
POINT IN TIME|FIRST OF JAN         |     1611|
POINT IN TIME|END OF DEC           |       60|
POINT IN TIME|FIRST OF MAY         |     8277|
POINT IN TIME|FIRST OF AUG         |     1733|
POINT IN TIME|END OF AUG           |       60|
POINT IN TIME|END OF JUL           |       60|
POINT IN TIME|MID JAN              |     2086|
POINT IN TIME|END OF OCT           |       60|
POINT IN TIME|END OF MAR           |       62|
POINT IN TIME|END OF NOV           |       60|
POINT IN TIME|MID SEP              |      298|
SEASON       |SUMMER - JUN FORECAST|      144|
SEASON       |FALL - SEP FORECAST  |       42|
SEASON       |SPRING - MAR FORECAST|       42|
SEASON       |WINTER - DEC FORECAST|       18|
WEEKLY       |WEEK #37             |    27096|
WEEKLY       |WEEK #09             |      454|
WEEKLY       |WEEK #51             |      144|
WEEKLY       |WEEK #43             |    18848|
WEEKLY       |WEEK #31             |    30712|
WEEKLY       |WEEK #27             |    30315|
WEEKLY       |WEEK #36             |    27042|
WEEKLY       |WEEK #14             |     9762|
WEEKLY       |WEEK #08             |      743|
WEEKLY       |WEEK #41             |    20666|
WEEKLY       |WEEK #49             |      649|
WEEKLY       |WEEK #19             |    17198|
WEEKLY       |WEEK #11             |      881|
WEEKLY       |WEEK #15             |    11691|
WEEKLY       |WEEK #01             |      188|
WEEKLY       |WEEK #17             |    15301|
WEEKLY       |WEEK #20             |    20538|
WEEKLY       |WEEK #21             |    23802|
WEEKLY       |WEEK #47             |     9447|
WEEKLY       |WEEK #33             |    28738|
WEEKLY       |WEEK #12             |     1721|
WEEKLY       |WEEK #52             |      314|
WEEKLY       |WEEK #53             |       14|
WEEKLY       |WEEK #22             |    28322|
WEEKLY       |WEEK #26             |    31413|
WEEKLY       |WEEK #42             |    20256|
WEEKLY       |WEEK #35             |    26931|
WEEKLY       |WEEK #38             |    26946|
WEEKLY       |WEEK #46             |    11327|
WEEKLY       |WEEK #28             |    29540|
WEEKLY       |WEEK #34             |    27782|
WEEKLY       |WEEK #04             |      562|
WEEKLY       |WEEK #23             |    31370|
WEEKLY       |WEEK #18             |    16799|
WEEKLY       |WEEK #13             |     6150|
WEEKLY       |WEEK #24             |    31975|
WEEKLY       |WEEK #48             |     2750|
WEEKLY       |WEEK #03             |       79|
WEEKLY       |WEEK #39             |    26640|
WEEKLY       |WEEK #29             |    29759|
WEEKLY       |WEEK #32             |    30212|
WEEKLY       |WEEK #40             |    23576|
WEEKLY       |WEEK #45             |    13633|
WEEKLY       |WEEK #50             |      219|
WEEKLY       |WEEK #05             |      221|
WEEKLY       |WEEK #06             |      114|
WEEKLY       |WEEK #25             |    31802|
WEEKLY       |WEEK #30             |    30060|
WEEKLY       |WEEK #16             |    13094|
WEEKLY       |WEEK #10             |      615|
WEEKLY       |WEEK #44             |    15886|
WEEKLY       |WEEK #02             |       58|
WEEKLY       |WEEK #07             |      122|
*/

/* Do we have all 50 states? */

SELECT
       state_name
FROM
       analyst_data.ag_exp
GROUP BY
       1 ;
       
/* We get 53 rows leaving us 3 non standard states
* Other States - probably us territories
* US Total - aggregates across all states
* NULL -
*/

/* Looking at the NULL 
* We see the data belongs to a different type of aggregated reporting which do not roll up into states
*/        
SELECT
       agg_level_desc 
FROM
       analyst_data.ag_exp
WHERE
       state_name IS NULL 
GROUP BY
       1 ;
/*
AGG_LEVEL_DESC             |
---------------------------|
WATERSHED                  |
REGION : MULTI-STATE       |
AMERICAN INDIAN RESERVATION|
INTERNATIONAL              |       
*/

/* Can states share county names? - yes
* Can verify externally and in the dataset 
* https://en.wikipedia.org/wiki/List_of_the_most_common_U.S._county_names
* this dataset provides enough information to construct the FIPS code to identify unique counties across states
* FIPS seems to no longer be a government standard 
* but it seems the census bureau has retained its usage and we can still use for our geographical purposes
*/
SELECT 
       county_name
,      COUNT(DISTINCT state_name )
FROM
       analyst_data.ag_exp
GROUP BY 
       1
ORDER BY 
       2 DESC ;

/* Do all 50 states produce one or more of corn | soy | wheat | cotton or rice?
* Yes - but limited survey data out of Alaska and Hawaii on the 5 primary crops we want to look at
* Could supplement with census data
*/

WITH state_commodity_year AS (
SELECT 
       state_name 
,      year
,      commodity_desc
,      year||'-'||commodity_desc                                                           AS year_commodity
FROM
       analyst_data.ag_exp
WHERE
       year >= 1990       
AND
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
GROUP BY
       1,2,3
       )
SELECT
       state_name
,      ARRAY_AGG(DISTINCT year) WITHIN GROUP (ORDER BY YEAR)                               AS production_years
,      ARRAY_AGG(DISTINCT commodity_desc) WITHIN GROUP (ORDER BY commodity_desc)           AS commodities_produced
,      ARRAY_AGG(year_commodity) WITHIN GROUP (ORDER BY year)                              AS commodities_produced_year
,      ARRAY_SIZE(commodities_produced)                                                    AS unique_commodities
FROM 
       state_commodity_year
GROUP BY 
       state_name
ORDER BY 
       unique_commodities DESC ;

SELECT 
       state_name
,      source_desc       
,      statisticcat_desc 
,      commodity_desc
,      unit_desc
,      value
FROM
       analyst_data.AG_EXP 
WHERE
       state_name IN ('ALASKA','HAWAII')
AND
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
AND
       source_desc = 'SURVEY'       
GROUP BY 
       1,2,3,4,5,6
ORDER BY
       1,2 ;   

/* We have an identifier for country for US data - seems we also have some data for Puerto Rico 
* 8 years of coffee data between 2003 and 2010 - nothing beyond 2010
*/
SELECT
       country_name
FROM
       analyst_data.ag_exp
GROUP BY
       1;

SELECT 
       *
FROM
       analyst_data.ag_exp
WHERE
       country_name = 'PUERTO RICO' ;



/* 
 * The values in the "value" column are stored as strings 
 * We can convert the columns that do not contain strings into numeric
*/

SELECT 
       MAX(REGEXP_COUNT(value,'(\,)'))                                                     AS max_place_value
,      MAX(LENGTH(regexp_substr(value,'\\d+\\.(\\d+)',1,1,'e')))                           AS max_decimal_places
FROM 
       analyst_data.ag_exp ;

/* 
* The max of any value in this dataset goes to the billions in place value (3 commas) 
* The max decimal places any value contains is also 3
* Knowing this, we can use 999,999,999.999  in Snowflakes format conversion functions and 3 digits for decimal places scale
*/



/* What units do each of our interested commodities use? */
SELECT
       statisticcat_desc
,      unit_desc
,      commodity_desc
FROM
       analyst_data.ag_exp
WHERE
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')       
AND
       source_desc = 'SURVEY'       
GROUP BY
       1,2,3
ORDER BY
       1,2,3 ;


/* This sample query helps validate how crops acres harvested roll up into state in 2007 from census data */ 
SELECT
       STATE_NAME
,      SUM(TRY_TO_NUMBER(VALUE,'999,999,999,999.999',38,3))                                AS acres_harvested
,      SUM(acres_harvested) OVER ()                                                        AS total_us_harvested
FROM analyst_data.ag_exp
WHERE COMMODITY_DESC  = 'CORN'
AND YEAR = 2007
AND domain_desc = 'TOTAL'
AND unit_desc = 'ACRES'
AND STATISTICCAT_DESC  = 'AREA HARVESTED'
AND SHORT_DESC = 'CORN, GRAIN - ACRES HARVESTED'
AND SOURCE_DESC = 'CENSUS'
AND REFERENCE_PERIOD_DESC = 'YEAR'
AND AGG_LEVEL_DESC = 'STATE'
GROUP BY 1
ORDER BY 1 ;


/* totals match up to this query */
SELECT
       STATE_NAME
,      SUM(TRY_TO_NUMBER(VALUE,'999,999,999,999.999'))                                     AS acres_harvested
,      SUM(acres_harvested) OVER ()                                                        AS total_us_harvested
FROM analyst_data.ag_exp
WHERE COMMODITY_DESC  = 'CORN'
AND YEAR = 2007
AND domain_desc = 'TOTAL'
AND unit_desc = 'ACRES'
AND STATISTICCAT_DESC  = 'AREA HARVESTED'
AND SHORT_DESC = 'CORN, GRAIN - ACRES HARVESTED'
AND SOURCE_DESC = 'CENSUS'
AND REFERENCE_PERIOD_DESC = 'YEAR'
AND AGG_LEVEL_DESC = 'NATIONAL'
GROUP BY 1
ORDER BY 1 ;



/*
* Setting up a temporary staging table with definitive criteria we know we'll use in the final table
* years after 1990
* and limited to 5 crops: corn, soy, wheat, cotton, and rice
* Sweet corn might be a subset but the final output only returns a limited 3 years worth of data
* Excluding for the cleaned dataset - but can easily bring back in 
*/

CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_core LIKE analyst_data.ag_exp;
INSERT INTO analyst_data.ag_exp_core
SELECT * FROM analyst_data.ag_exp
WHERE year >= 1990
AND REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie')
AND NOT commodity_desc = 'SWEET CORN' ;

/* Taking stock of some points in the dataset to cleanup
* Glancing over some of the raw data - we can see that dataset includes a rich amount of information to breakup
* The dataset blends together census and survey data
       * the census occurs every 5 years and we can likely rely on the survey data for reporting and analysis
* When comparing states - a few things can roll into each other
* Should follow a roll up similar to a specific location > county > state
       * e.g. The aggregated total for US Total is a field included along with other states
       * A number of other totals fields exist
       * We can settle on using one or the other for a final dataset 
              * For the use case of this dashboard we can stick to just totals
              * More details allows more options for diving into specific items and would not be a big lift to add later
* Commodities can also have sub items
       *        
* Counties occasionally have data omitted represented through by (D) rather than a numeric value - PII concerns
* Rolling counties into state likely does match perfectly when aggregated because of this data omission
       * Create a date column using year to help dashboards that will layer on top
       * We could create an "Unknown" county if desired to tie to state and national level reporting
       * Using unknown would primarily benefit reporting but not on geographical visuals
*/

/* census data included every 5 years over states numbers 
*  can filter out or substitute can filter out CENSUS data for the cleaned dataset */
SELECT 
       year
FROM 
       analyst_data.ag_exp 
WHERE 
       source_desc = 'CENSUS'
GROUP BY
       1
ORDER BY
       1 ;

SELECT 
       commodity_desc 
,      statisticcat_desc 
,      short_desc
,      class_desc 
,      util_practice_desc 
,      unit_desc 
,      prodn_practice_desc
,      domain_desc
,      SPLIT_PART(short_desc,' - ', 1)                                                     AS commodity_group
,      COALESCE(
              NULLIF(SPLIT_PART(commodity_group,',', 3),'')
       ,      NULLIF(SPLIT_PART(commodity_group,',', 2),'')
       )                                                                                   AS commodity_variety
,      SUM(TRY_TO_NUMBER(VALUE,'999,999,999,999.999',38,3))                                AS value_number
,      SUM(value_number) OVER (PARTITION BY commodity_desc, statisticcat_desc,unit_desc)   AS total_value
FROM analyst_data.ag_exp
WHERE
       -- year >= 1990
       year = 2019
AND
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
--AND unit_desc = 'ACRES'
--AND STATISTICCAT_DESC  = 'PRODUCTION'
--AND STATISTICCAT_DESC ='AREA PLANTED'
--AND SHORT_DESC = 'CORN, GRAIN - ACRES HARVESTED'
AND
       source_desc = 'SURVEY'
AND
       reference_period_desc = 'YEAR'
AND
       agg_level_desc = 'COUNTY'
-- AND
--        prodn_practice_desc  = 'ALL PRODUCTION PRACTICES'       
ANd
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')       
GROUP BY
       1,2,3,4,5,6,7,8,9
ORDER BY
       commodity_desc, statisticcat_desc ;


/* this looks different at the state level - all classes included for wheat */ 
SELECT 
       commodity_desc 
,      statisticcat_desc 
,      short_desc
,      class_desc 
,      util_practice_desc 
,      unit_desc 
,      prodn_practice_desc
,      domain_desc
,      SPLIT_PART(short_desc,' - ', 1)                                                     AS commodity_group
,      COALESCE(
              NULLIF(SPLIT_PART(commodity_group,',', 3),'')
       ,      NULLIF(SPLIT_PART(commodity_group,',', 2),'')
       )                                                                                   AS commodity_variety
,      SUM(TRY_TO_NUMBER(VALUE,'999,999,999,999.999',38,3))                                AS value_number
,      SUM(value_number) OVER (PARTITION BY commodity_desc, statisticcat_desc,unit_desc)   AS total_value
FROM analyst_data.ag_exp
WHERE
       -- year >= 1990
       year = 2019
AND
       REGEXP_LIKE(commodity_desc,'.*(corn|soy|wheat|cotton|rice).*','ie') 
--AND unit_desc = 'ACRES'
--AND STATISTICCAT_DESC  = 'PRODUCTION'
--AND STATISTICCAT_DESC ='AREA PLANTED'
--AND SHORT_DESC = 'CORN, GRAIN - ACRES HARVESTED'
AND
       source_desc = 'SURVEY'
AND
       reference_period_desc = 'YEAR'
AND
       agg_level_desc = 'STATE'
AND
       prodn_practice_desc  = 'ALL PRODUCTION PRACTICES'       
ANd
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')       
GROUP BY
       1,2,3,4,5,6,7,8,9
ORDER BY
       commodity_desc, statisticcat_desc ;

/* We can exclude all classes later for cotton and wheat - corn depends on all classes oddly for area planted
* Data not provided for area planted at the county level for corn
*/

WITH crops_with_variety AS (
SELECT
       commodity_desc 
FROM
       analyst_data.ag_exp_core
GROUP BY
       1
HAVING
       COUNT(DISTINCT class_desc)>1
       )
SELECT 
       ag_exp_core.commodity_desc 
,      ag_exp_core.class_desc
FROM
       analyst_data.ag_exp_core
JOIN 
       crops_with_variety
ON
       ag_exp_core.commodity_desc = crops_with_variety.commodity_desc
GROUP BY
       1,2
ORDER BY
       1,2 ;


/*
* Census seems to omit or does not collect data for acres planted
* Need to rely on the survey data for acres planted
*/

/* clean up
* can do some light clean up of values to make more visually presentable 
* prefer to leave data in its raw form
 */

CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_clean AS 
SELECT 
       year
,      TO_DATE(year||'-01-01')                                                             AS year_date
,      commodity_desc
,      class_desc
,      prodn_practice_desc
,      util_practice_desc
,      unit_desc
,      state_alpha
,      state_name
,      SUM(
              CASE 
                     WHEN source_desc = 'CENSUS' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'AREA PLANTED'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_planted_census
,      SUM(
              CASE 
                     WHEN source_desc = 'SURVEY' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'AREA PLANTED'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_planted_survey
,      SUM(
              CASE 
                     WHEN source_desc = 'CENSUS' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'AREA HARVESTED'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_harvested_census
,      SUM(
              CASE 
                     WHEN source_desc = 'SURVEY' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'AREA HARVESTED'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_harvested_survey
,      SUM(
              CASE 
                     WHEN source_desc = 'CENSUS' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'YIELD'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_yield_census
,      SUM(
              CASE 
                     WHEN source_desc = 'SURVEY' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'YIELD'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_yield_survey
,      SUM(
              CASE 
                     WHEN source_desc = 'CENSUS' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'PRODUCTION'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_production_census
,      SUM(
              CASE 
                     WHEN source_desc = 'SURVEY' 
                     AND domain_desc = 'TOTAL'
                     AND statisticcat_desc = 'PRODUCTION'
                     THEN TRY_TO_NUMBER(value,'999,999,999,999.999')
              END                     
       )                                                                                   AS area_production_survey
FROM
       analyst_data.ag_exp_core
WHERE
       -- agg_level_desc = 'STATE'
       -- agg_level_desc = 'NATIONAL'
       agg_level_desc = 'COUNTY'
-- AND
--        commodity_desc = 'CORN'
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
AND
       unit_desc = 'ACRES'       
-- AND
       -- prodn_practice_desc = 'ALL PRODUCTION PRACTICES'       
GROUP BY 
       1,2,3,4,5,6,7,8,9 ;



/* Corn is an odd commodity and identifies a variation through util description
* Can address this in at least a couple of ways for the cleaned dataset
*      1. Leave the commodity structure as is and allow for totals mixed in with variety and selecting each independently
       This works well as is for a dashboard but multiple aggregate levels can cause confusion for end users
*      2. Separate into a commodity grouping with a variety  and / or class as a separate option
       This seems to fit the better usability and would likely lead to less confusion for an end user
*/

SELECT
       commodity_desc 
FROM
       analyst_data.ag_exp_clean
WHERE
       util_practice_desc <> 'ALL UTILIZATION PRACTICES'
GROUP BY
       1 ;

SELECT
       commodity_desc 
FROM
       analyst_data.AG_EXP_CLEAN 
WHERE
       class_desc <> 'ALL CLASSES'
GROUP BY
       1 ;

/* Oddly the dataset only contains information for different rice varieties at the state and notional level 
* Leave rice aggregated to all classes for comparing totals to the state and national level
* Corn also needs to be treated differently
       * Grain vs silage are not technically considered varieties, but acreage still rolls up into Corn
       * Corn typically measured in bushels
* Conversion reference - https://www.ers.usda.gov/webdocs/publications/41880/33132_ah697_002.pdf?v=0       
* Using the usda conversion table - can bring roughly standardize and convert 1 us short ton to 56 lb bushels 
* (2000lb/ton / 56lb/bushel = 35.714 bushels per ton)
* $ amounts not recorded at the county level - omitting from cleaned county dataset
* the first attempt at a clean data set still needs some updates
*/

CREATE OR REPLACE LOCAL TEMP TABLE analyst_data.ag_exp_known_county AS 
WITH crops_with_variety AS (
SELECT
       commodity_desc
,      'ALL CLASSES'                                                                       AS class_desc       
FROM
       analyst_data.ag_exp_core
WHERE
       -- NOT REGEXP_LIKE(commodity_desc,'.*(CORN).*')
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
              THEN 'AREA HARVESTED (ONLY)'
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
       -- agg_level_desc = 'STATE'
       -- agg_level_desc = 'NATIONAL'
       agg_level_desc = 'COUNTY'
AND
       source_desc = 'SURVEY'       
-- AND
--        commodity_desc = 'CORN'
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
-- AND
       -- unit_desc = 'ACRES'       
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
              THEN 'AREA HARVESTED (ONLY)'
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
       -- agg_level_desc = 'NATIONAL'
       -- agg_level_desc = 'COUNTY'
AND
       source_desc = 'SURVEY'       
-- AND
--        commodity_desc = 'CORN'
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
-- AND
       -- unit_desc = 'ACRES'       
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
              THEN 'AREA HARVESTED (ONLY)'
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
       -- agg_level_desc = 'STATE'
       -- agg_level_desc = 'COUNTY'
AND
       source_desc = 'SURVEY'       
-- AND
--        commodity_desc = 'CORN'
AND
       statisticcat_desc IN ('AREA HARVESTED', 'AREA PLANTED', 'YIELD', 'PRODUCTION')
AND
       reference_period_desc  = 'YEAR'
-- AND
       -- unit_desc = 'ACRES'       
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




/* Crop totals by variety and year now line up at the county - state and national levels */

SELECT 
       YEAR 
,      sum(area_harvested) harvested
,      sum(area_planted) planted
,      sum(crop_production) production
,      production/harvested AS yield
FROM analyst_data.ag_exp_clean_county
--WHERE commodity_name = 'Wheat'
-- WHERE commodity_name = 'Corn'
--AND commodity_class_variety = 'Grain'
--WHERE commodity_name = 'Rice'
--WHERE commodity_name = 'Cotton'
WHERE commodity_name = 'Soybeans'
GROUP BY 1
ORDER BY 1 ;

SELECT 
       YEAR 
,      sum(area_harvested) harvested
,      sum(area_planted) planted
,      sum(crop_production) production
,      sum(crop_production*production_unit_multiplier) production_bu
,      production/harvested AS yield
,      production*35.714
FROM analyst_data.ag_exp_clean_national
--WHERE commodity_name = 'Wheat'
--WHERE commodity_name = 'Corn'
--AND commodity_class_variety = 'Silage'
--WHERE commodity_name = 'Rice'
--WHERE commodity_name = 'Cotton'
WHERE commodity_name = 'Soybeans'
GROUP BY 1
ORDER BY 1 ;


/*
stage a csv file in snowflake to export 
could split into many files - but choosing to keep as a single file for easier access
*/


/* export cleaned dataset as a text file using the same tab delimiter as the original data file */
COPY INTO @analyst_data.%ag_exp_clean_county/ag_exp_clean_county.txt.gz
FROM analyst_data.ag_exp_clean_county
FILE_FORMAT = (
        type = CSV
        field_delimiter = '\t'
        -- field_delimiter = '|'
--        field_optionally_enclosed_by = '"'
        escape = '\\'
        null_if = ('','NULL','null','\\N')
        empty_field_as_null = FALSE
        compression = gzip
--        compression = NONE
        file_extension = 'gz'
--       file_extension = 'txt'
    )
HEADER = TRUE
overwrite = TRUE 
single = TRUE ;

/* Can check payload */ 
LIST @analyst_data.%ag_exp_clean_county ;

/* Save to local directory */
GET @analyst_data.%ag_exp_clean_county/ag_exp_clean_county file://C:\Users\matth\downloads ;


/* row count exported and brought into visual should match table row count */
SELECT 
       COUNT(*)
FROM
       analyst_data.ag_exp_clean_county
       ;
-- 255867