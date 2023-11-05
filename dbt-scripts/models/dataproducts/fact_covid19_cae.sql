{{
    config(
        alias = 'fact_covid19_case'
        ,unique_key='__key_covid_ts_id'
        ,materialized='incremental'
        ,on_schema_change='append_new_columns'
    )
}}

with covid19_time_series as
( 
  select 
         date_recorded
        ,country_region 
        ,sum(nvl(confirmed_cnt,0)) as confirmed_cnt
        ,sum(nvl(recovered_cnt,0)) as recovered_cnt
        ,sum(nvl(deaths_cnt,0))    as deaths_cnt
  from {{ ref('stg_covid19__covid_time_series_combined')}}  
  group by  date_recorded
            ,country_region
  
)
,dates as
(
    select __key_date_id 
    from {{ ref('dim_date')}}  
)
,countries as
(
    select __key_country_id
          , country_name    
          , total_population_cnt 
    from {{ ref('dim_country')}} 
)
, main as
(
select 
       md5(cast(ts.date_recorded as varchar)||ts.country_region)                as __key_covid_ts_id
      ,__key_date_id
      ,__key_country_id
      ,date_recorded                                                            as recorded_at_dt
      ,confirmed_cnt
      ,recovered_cnt
      ,deaths_cnt
      ,round(nvl(deaths_cnt,0)/iff(confirmed_cnt=0 or confirmed_cnt is null,1,confirmed_cnt),4)             as death_percentage
      ,round(nvl(recovered_cnt,0)/iff(confirmed_cnt=0 or confirmed_cnt is null,1,confirmed_cnt),4)          as recovered_percentage
      ,round(nvl(deaths_cnt,0)*1000/iff(confirmed_cnt=0 or confirmed_cnt is null,1,confirmed_cnt),4)         as death_rate_1000
      ,round(nvl(confirmed_cnt,0)*1000/iff(countries.total_population_cnt=0 or countries.total_population_cnt is null,1,countries.total_population_cnt),4)  as confirmed_percentage
from covid19_time_series ts 
left join dates on dates.__key_date_id=ts.date_recorded
left join countries on countries.country_name=ts.country_region
)
select * from main
