{{ config(materialized='table') }}

with source_albums as (
    select
            id::integer as album_id,
            band as band_id,
            trim(title) as album_title,
            year::integer as album_year
    from {{ source('metal_data', 'metal_albums') }}
    where id is not null and band is not null
)

select *
from source_albums