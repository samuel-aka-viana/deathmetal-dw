-- mart_subgenre_analysis.sql
{{ config(materialized='view') }}

with subgenre_metrics as (
    select
        b.death_metal_subgenre,
        b.continent,

        -- Contadores básicos
        count(distinct b.band_id) as total_bands,
        count(distinct a.album_id) as total_albums,
        count(r.review_id) as total_reviews,

        -- Métricas de qualidade
        avg(r.score_album) as avg_score,
        percentile_cont(0.5) within group (order by r.score_album) as median_score,
        stddev(r.score_album) as score_variance,

        -- Distribuição de qualidade
        sum(case when r.score_album >= 90 then 1 else 0 end) as excellent_count,
        sum(case when r.score_album >= 80 then 1 else 0 end) as high_quality_count,
        sum(case when r.score_album < 50 then 1 else 0 end) as poor_count,

        -- Percentuais
        sum(case when r.score_album >= 90 then 1 else 0 end) * 100.0 / count(r.review_id) as pct_excellent,
        sum(case when r.score_album >= 80 then 1 else 0 end) * 100.0 / count(r.review_id) as pct_high_quality,

        -- Análise temporal
        min(b.formed_year) as earliest_band,
        max(b.formed_year) as latest_band,
        mode() within group (order by a.release_decade) as dominant_decade,

        -- Atividade
        sum(b.is_active_flag) as active_bands,
        sum(b.is_veteran_band) as veteran_bands,

        -- Países dominantes
        count(distinct b.country) as countries_count,
        mode() within group (order by b.country) as dominant_country

    from {{ ref('dim_bands') }} b
    INNER join {{ ref('dim_albums') }} a on b.band_id = a.band_id
    left join {{ ref('fct_reviews') }} r on a.album_id = r.album_id
    where b.death_metal_subgenre is not null
    group by 1,2
)

select *,
    rank() over (order by avg_score desc) as quality_rank,
    rank() over (order by total_bands desc) as popularity_rank,
    rank() over (order by pct_excellent desc) as excellence_rank
from subgenre_metrics