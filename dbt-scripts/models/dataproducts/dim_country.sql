{{ config(
	 alias = 'dim_country'
    ,unique_key='__key_country_id'
    ,on_schema_change='append_new_columns'
  ) 
}}

with country as
            ( 
              select 
                     country_region          as country_name
                    ,sum(population_cnt)    as total_population_cnt
              from {{ ref('stg_covid19__covid_reference_data')}}  
              group by country_region
            ),
      main as
            (
                select 
                     md5(country.country_name||total_population_cnt)  as __key_country_id
                    ,country_name
                    ,total_population_cnt
                from country
                union all
                select md5('<NS>')                                     as _key_country_id
                      ,'<NOT SPECIFIED>'                               as country_name
                      ,-1                                              as total_population_cnt
               
            )      

select * from main
