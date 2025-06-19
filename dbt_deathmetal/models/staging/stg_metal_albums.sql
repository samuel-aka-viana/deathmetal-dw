-- models/staging/stg_metal_albums.sql
-- Cross-platform compatible version

{{ config(materialized='table') }}

with source_albums as (
    select
        {{ safe_cast_integer('id') }} as album_id,
        band as band_id,
        trim(title) as album_title,
        {{ safe_cast_integer('year') }} as album_year
    from {{ source('metal_data', 'metal_albums') }}
    where id is not null and band is not null
)

select *
from source_albums