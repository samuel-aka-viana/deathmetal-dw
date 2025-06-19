{{ config(materialized='view') }}

with base as (
    select
        a.album_year,
        a.release_decade,
        a.release_era,
        a.album_id,
        a.band_id,
        b.country,
        b.death_metal_subgenre,
        a.is_debut_album,
        r.review_id,
        r.score_album
    from {{ ref('dim_albums') }} a
    join {{ ref('dim_bands') }} b
      on a.band_id = b.band_id
    left join {{ ref('fct_reviews') }} r
      on a.album_id = r.album_id
    where a.album_year between 1980 and 2024
),

aggregated as (
    select
        album_year,
        release_decade,
        release_era,
        count(distinct album_id)         as albums_released,
        count(distinct case when review_id is not null then band_id end) as active_bands,
        count(distinct country)          as countries_represented,
        count(distinct case when is_debut_album = 1 then album_id end) as debut_albums,
        avg(case when is_debut_album = 1 then score_album end) as avg_debut_score,
        count(distinct death_metal_subgenre) as subgenres_active
    from base
    group by album_year, release_decade, release_era
),

score_stats as (
    select
        album_year,
        avg(score_album) over (partition by album_year)    as avg_score,
        percentile_cont(score_album, 0.5) over (partition by album_year) as median_score,
        stddev(score_album) over (partition by album_year) as score_variance,
        sum(case when score_album >= 90 then 1 else 0 end) over(partition by album_year) as excellent_albums,
        sum(case when score_album >= 80 then 1 else 0 end) over(partition by album_year) as high_quality_albums,
        sum(case when score_album < 50 then 1 else 0 end) over(partition by album_year) as poor_albums
    from base
),

subgenre_count as (
    select
        album_year,
        death_metal_subgenre,
        count(*) as cnt
    from base
    where death_metal_subgenre is not null
    group by album_year, death_metal_subgenre
),

dominant_subgenre_cte as (
    select
        album_year,
        death_metal_subgenre as dominant_subgenre
    from (
        select
            album_year,
            death_metal_subgenre,
            row_number() over (
                partition by album_year
                order by cnt desc, death_metal_subgenre
            ) as rn
        from subgenre_count
    )
    where rn = 1
),

final as (
    select
        a.*,
        s.avg_score,
        s.median_score,
        s.score_variance,
        s.excellent_albums,
        s.high_quality_albums,
        s.poor_albums,
        d.dominant_subgenre
    from aggregated a
    left join (
        select distinct
            album_year,
            avg_score,
            median_score,
            score_variance,
            excellent_albums,
            high_quality_albums,
            poor_albums
        from score_stats
    ) s
      using (album_year)
    left join dominant_subgenre_cte d
      using (album_year)
)

select
    *,
    {{ get_row_number(order_by='albums_released desc') }}    as release_rank,
    {{ get_row_number(order_by='avg_score desc') }}           as quality_rank
from final
order by album_year
