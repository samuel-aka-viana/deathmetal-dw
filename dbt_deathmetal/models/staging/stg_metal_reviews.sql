{{ config(materialized='table') }}

with source_reviews as (
    select
        {{ safe_cast_integer('id') }} as review_id,
        album as album_id,
        trim(title) as review_title,
        {{ safe_cast_decimal('score') }} as score_album,
        content as content_review
    from {{ source('metal_data', 'metal_reviews') }}
    where album is not null
)

select *
from source_reviews