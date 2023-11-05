
-- 1. create schemas for covid data flow
use database dev_rds;

create schema if not exists covid;

use database dev_hds;

create schema if not exists covid;

use database dev_ods;

create schema if not exists covid;

use database dev_dataproducts;

create schema if not exists covid;

-- 2. prepare data for load

use database dev_rds;
use schema covid;

-- 2.1. create csv file format

create or replace file format ff_dev_csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true
  COMPRESSION = none
  ;

-- 2.2. create stage, enable directory

create  or replace stage stg_dev_covid
 directory= (enable=true);

-- 2.3. create and load source table
-- 2.3.1. time series data

create or replace table covid_time_series_combined
(
date_recorded varchar(30),
country_Region varchar(200),
province_State varchar(200),
confirmed_cnt varchar(50),
recovered_cnt varchar(50),
deaths_cnt varchar(50),
date_loaded timestamp_ntz default current_timestamp(),
source_file varchar(50)
)
comment = 'Covid 19 raw data - time series combined';


-- 2.3.2. reference data
create or replace table covid_reference_data
(
UID varchar(100),
iso2 varchar(100),
iso3 varchar(100),
code3 varchar(100),
FIPS varchar(100),
Admin2 varchar(100),
Province_State varchar(200),
Country_Region varchar(200),
Lat varchar,
Long_ varchar,
Combined_Key varchar,
Population_cnt varchar(50),
date_loaded timestamp_ntz default current_timestamp(),
source_file varchar(50)
)
comment = 'Covid 19 raw data - reference data';


list @stg_dev_covid;


/*
stg_dev_covid/reference.csv
UID,iso2,iso3,code3,FIPS,Admin2,Province_State,Country_Region,Lat,Long_,Combined_Key,Population


stg_dev_covid/time-series-19-covid-combined.csv

Date,Country/Region,Province/State,Confirmed,Recovered,Deaths
*/

-- 2.3.3. load source tables

 copy into covid_time_series_combined
 (date_recorded, country_Region,province_State,confirmed_cnt,recovered_cnt,deaths_cnt,source_file)
 from 
 (
 select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,METADATA$FILENAME
 from @stg_dev_covid/time-series-19-covid-combined.csv
   (file_format=>ff_dev_csv) t
  );


copy into covid_reference_data
 (UID ,iso2,iso3,code3,FIPS ,Admin2,Province_State,Country_Region,Lat,Long_ ,Combined_Key ,Population_cnt, source_file)
 from 
 (
 select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11,t.$12,METADATA$FILENAME
 from @stg_dev_covid/reference.csv
   (file_format=>ff_dev_csv) t
  );


