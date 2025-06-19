{{ config(materialized='view') }}

with geographic_metrics as (
    select
        b.country,
        b.continent,

        count(distinct b.band_id) as total_bands,
        count(distinct a.album_id) as total_albums,

        min(b.formed_year) as first_band_formed,
        max(b.formed_year) as latest_band_formed,

        mode() within group (order by b.formation_era) as dominant_era,

        count(distinct b.death_metal_subgenre) as subgenres_count,
        mode() within group (order by b.death_metal_subgenre) as dominant_subgenre,

        sum(b.is_active_flag) as active_bands,
        sum(b.is_veteran_band) as veteran_bands,


        count(distinct a.album_id) * 1.0 / count(distinct b.band_id) as albums_per_band,

        sum(case when a.release_decade = '1990s' then 1 else 0 end) as albums_90s,
        sum(case when a.release_decade = '2000s' then 1 else 0 end) as albums_00s,
        sum(case when a.release_decade = '2010s' then 1 else 0 end) as albums_10s

    from {{ ref('dim_bands') }} b
    left join {{ ref('dim_albums') }} a on b.band_id = a.band_id
    group by 1,2
)

select *,
    rank() over (order by total_bands desc) as country_quantity_rank
from geographic_metrics