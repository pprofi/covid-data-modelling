with 

source as (

    select * from {{ source('covid19', 'covid_time_series_combined') }}

),

renamed as (

    select
        to_date(date_recorded,'yyyy-mm-dd') as date_recorded,
        country_region,
        province_state,
        cast(confirmed_cnt as number) as confirmed_cnt,
        cast(recovered_cnt as number) as recovered_cnt,
        cast(deaths_cnt as number) as deaths_cnt,
        date_loaded,
        source_file

    from source

)

select * from renamed
