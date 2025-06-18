{{ config(materialized='table') }}

with source_reviews as (
    select
            id::integer as review_id,
            album as album_id,
            trim(title) as review_title,
            score::decimal as score_album,
            content as content_review
    from {{ source('metal_data', 'metal_reviews') }}
    where album is not null
)

select *
from source_reviews