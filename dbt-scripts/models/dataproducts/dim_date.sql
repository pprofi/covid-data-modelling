{{ config(
	alias = 'dim_date'
	,target_schema=generate_schema_name('common')
    ,materialized = "table"
  ) 
}}

with 
    date_spine as
    (
    {{ dbt_utils.date_spine(
                        datepart="day",
                        start_date="to_date('01/01/2019', 'mm/dd/yyyy')",
                        end_date="TO_DATE(CONCAT(YEAR(CURRENT_DATE) + 6,'-12-31'))"
    )
    }}
    )
, step1 as 
(
select
            CAST(DATE_DAY AS DATE) AS __key_date_id
            ,CAST(DAYOFYEAR(__key_date_id) AS INT) AS DAY_OF_YEAR
            ,CAST(YEAR(__key_date_id) || RIGHT('0' || WEEK(__key_date_id), 2) AS INT) AS WEEK_KEY
            ,CAST(WEEKOFYEAR(__key_date_id) AS INT) AS WEEK_OF_YEAR
            ,CAST(DAYOFWEEK(__key_date_id) AS INT) AS DAY_OF_WEEK
            ,CAST(DAYNAME(__key_date_id) AS VARCHAR(5)) AS WEEK_DAY_SHORT_DESC
            ,CAST(DECODE (EXTRACT(DAYOFWEEK FROM __key_date_id),
            1 , 'Sunday',
            2 , 'Monday',
            3 , 'Tuesday',
            4 , 'Wednesday',
            5 , 'Thursday',
            6 , 'Friday',
            7 , 'Saturday'
            ) AS VARCHAR(9)) AS WEEK_DAY_DESC
            ,CAST(TRUNC(__key_date_id, 'Week') AS DATE) AS FIRST_DAY_OF_WEEK
            ,CAST(LAST_DAY(__key_date_id, 'Week') AS DATE) AS LAST_DAY_OF_WEEK
            ,CAST(YEAR(__key_date_id) || RIGHT('0' || MONTH(__key_date_id), 2) AS INT) AS MONTH_KEY
            ,CAST(MONTH(__key_date_id) AS INT) AS MONTH_OF_YEAR
            ,CAST(DAYOFMONTH(__key_date_id) AS INT) AS DAY_OF_MONTH
            ,CAST(MONTHNAME(__key_date_id) AS VARCHAR(5)) AS MONTH_SHORT_DESC
            ,CAST(TO_CHAR(__key_date_id, 'MMMM') AS VARCHAR(50)) AS MONTH_DESC
            ,CAST(TRUNC(__key_date_id, 'Month') AS DATE) AS FIRST_DAY_OF_MONTH
            ,CAST(LAST_DAY(__key_date_id, 'Month') AS DATE) AS LAST_DAY_OF_MONTH
            ,CAST(YEAR(__key_date_id) || QUARTER(__key_date_id) AS INT) AS QUARTER_KEY
            ,CAST(QUARTER(__key_date_id) AS INT) AS QUARTER_OF_YEAR
            ,CAST(__key_date_id - TRUNC(__key_date_id, 'Quarter') + 1 AS INT) AS DAY_OF_QUARTER
            ,CAST('Q' || QUARTER_OF_YEAR AS VARCHAR(5)) AS QUARTER_SHORT_DESC
            ,CAST('Quarter ' || QUARTER_OF_YEAR AS VARCHAR(50)) AS QUARTER_DESC
            ,CAST(TRUNC(__key_date_id, 'Quarter') AS DATE) AS FIRST_DAY_OF_QUARTER
            ,CAST(LAST_DAY(__key_date_id, 'Quarter') AS DATE) AS LAST_DAY_OF_QUARTER
            ,CAST(YEAR(__key_date_id) AS INT) AS YEAR_KEY
            ,CAST(TRUNC(__key_date_id, 'Year') AS DATE) AS FIRST_DAY_OF_YEAR
            ,CAST(LAST_DAY(__key_date_id, 'Year') AS DATE) AS LAST_DAY_OF_YEAR
from date_spine
),
main as -- not identified date
(
select *
from step1
union 
select
             cast('9999-01-01' as date)  AS __key_date_id
            ,null as  DAY_OF_YEAR
            ,null AS WEEK_KEY
            ,null AS WEEK_OF_YEAR
            ,null AS DAY_OF_WEEK
            ,null AS WEEK_DAY_SHORT_DESC
            ,null AS WEEK_DAY_DESC
            ,null AS FIRST_DAY_OF_WEEK
            ,null AS LAST_DAY_OF_WEEK
            ,null AS MONTH_KEY
            ,null AS MONTH_OF_YEAR
            ,null AS DAY_OF_MONTH
            ,null AS MONTH_SHORT_DESC
            ,null AS MONTH_DESC
            ,null AS FIRST_DAY_OF_MONTH
            ,null AS LAST_DAY_OF_MONTH
            ,null AS QUARTER_KEY
            ,null AS QUARTER_OF_YEAR
            ,null AS DAY_OF_QUARTER
            ,null AS QUARTER_SHORT_DESC
            ,null AS QUARTER_DESC
            ,null AS FIRST_DAY_OF_QUARTER
            ,null AS LAST_DAY_OF_QUARTER
            ,null AS YEAR_KEY
            ,null AS FIRST_DAY_OF_YEAR
            ,null AS LAST_DAY_OF_YEAR


)

select * from main




