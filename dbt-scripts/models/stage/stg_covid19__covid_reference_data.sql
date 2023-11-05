with 

source as (

    select * from {{ source('covid19', 'covid_reference_data') }}

),

renamed as (

    select
        uid,
        iso2,
        iso3,
        code3,
        fips,
        admin2,
        province_state,
        country_region,
        lat,
        long_,
        combined_key,
        nvl(cast(population_cnt as number),0) as population_cnt,
        date_loaded,
        source_file

    from source

)

select * from renamed
