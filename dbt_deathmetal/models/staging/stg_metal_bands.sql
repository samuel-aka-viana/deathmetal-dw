{{ config(materialized='table') }}

select id::integer as band_id, trim(name) as band_name,
       trim(country) as country,
       trim(status)  as status,
       formed_in::integer as formed_year, trim(genre) as genre,
       trim(theme)   as theme,
       trim(active)  as active_periods
from {{ source('metal_data', 'metal_bands') }}
where id is not null