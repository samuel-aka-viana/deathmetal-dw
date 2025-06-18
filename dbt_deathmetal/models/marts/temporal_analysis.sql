-- mart_temporal_analysis.sql
{{ config(materialized='view') }}

with yearly_metrics as (
    select
        a.album_year,
        a.release_decade,
        a.release_era,

        -- Contadores
        count(distinct a.album_id) as albums_released,
        count(distinct a.band_id) as active_bands,
        count(distinct b.country) as countries_represented,

        -- Métricas de qualidade
        avg(r.score_album) as avg_score,
        percentile_cont(0.5) within group (order by r.score_album) as median_score,
        stddev(r.score_album) as score_variance,

        -- Distribuição de scores
        sum(case when r.score_album >= 90 then 1 else 0 end) as excellent_albums,
        sum(case when r.score_album >= 80 then 1 else 0 end) as high_quality_albums,
        sum(case when r.score_album < 50 then 1 else 0 end) as poor_albums,

        -- Análise de debuts
        sum(a.is_debut_album) as debut_albums,
        avg(case when a.is_debut_album = 1 then r.score_album end) as avg_debut_score,

        -- Subgêneros
        count(distinct b.death_metal_subgenre) as subgenres_active,
        mode() within group (order by b.death_metal_subgenre) as dominant_subgenre

    from {{ ref('dim_albums') }} a
    join {{ ref('dim_bands') }} b on a.band_id = b.band_id
    left join {{ ref('fct_reviews') }} r on a.album_id = r.album_id
    where a.album_year between 1980 and 2024
    group by 1,2,3
)

select * from yearly_metrics
order by album_year