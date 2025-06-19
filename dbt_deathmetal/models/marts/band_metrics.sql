{{ config(materialized='view') }}

with band_metrics as (select b.band_id,
                             b.band_name,
                             b.country,
                             b.continent,
                             b.death_metal_subgenre,
                             b.formation_era,
                             b.band_maturity,
                             b.years_active,

                             count(distinct a.album_id)                                                        as total_albums,
                             min(a.album_year)                                                                 as first_album_year,
                             max(a.album_year)                                                                 as latest_album_year,
                             max(a.album_year) - min(a.album_year)                                             as discography_span,

                             count(r.review_id)                                                                as total_reviews,
                             avg(r.score_album)                                                                as avg_score,
                             min(r.score_album)                                                                as min_score,
                             max(r.score_album)                                                                as max_score,
                            {{ stddev_function('r.score_album') }} as score_consistency,

                            {% if target.type == 'bigquery' %}
                                SAFE_DIVIDE(sum(case when r.score_album >= 80 then 1 else 0 end) * 100.0, count(r.review_id))
                            {% else %}
                                sum(case when r.score_album >= 80 then 1 else 0 end) * 100.0 / count(r.review_id)
                            {% endif %} as pct_high_quality,

                            {% if target.type == 'bigquery' %}
                                SAFE_DIVIDE(sum(case when r.score_album >= 90 then 1 else 0 end) * 100.0, count(r.review_id))
                            {% else %}
                                sum(case when r.score_album >= 90 then 1 else 0 end) * 100.0 / count(r.review_id)
                            {% endif %} as pct_excellent,

                             case when count(r.review_id) >= 5 then 1 else 0 end                               as has_substantial_catalog,
                            case when {{ stddev_function('r.score_album') }} <= 10 then 1 else 0 end as is_consistent_quality
                      from {{ ref('dim_bands') }} b
                               left join {{ ref('dim_albums') }} a on b.band_id = a.band_id
                               left join {{ ref('fct_reviews') }} r on a.album_id = r.album_id
                      group by 1, 2, 3, 4, 5, 6, 7, 8)

select *
from band_metrics