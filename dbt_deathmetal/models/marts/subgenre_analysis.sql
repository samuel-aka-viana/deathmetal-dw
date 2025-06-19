{{ config(materialized='table') }}

with base as (
    select
        b.band_id,
        b.death_metal_subgenre,
        b.continent,
        b.country,
        b.formed_year,
        b.is_active_flag,
        b.is_veteran_band,
        a.album_id,
        a.release_decade,
        r.review_id,
        r.score_album
    from {{ ref('dim_bands') }} b
    join {{ ref('dim_albums') }} a
      on b.band_id = a.band_id
    left join {{ ref('fct_reviews') }} r
      on a.album_id = r.album_id
    where b.death_metal_subgenre is not null
),

aggregated as (
    select
        death_metal_subgenre,
        continent,
        count(distinct album_id) as total_albums,
        count(distinct review_id) as total_reviews,
        count(distinct band_id) as total_bands,
        avg(score_album) as avg_score,
        {% if target.type == 'bigquery' %}
            stddev(score_album) as score_variance,
        {% else %}
            stddev_samp(score_album) as score_variance,
        {% endif %}
        sum(case when score_album >= 90 then 1 else 0 end) as excellent_count,
        sum(case when score_album >= 80 then 1 else 0 end) as high_quality_count,
        sum(case when score_album < 50 then 1 else 0 end) as poor_count,
        {% if target.type == 'bigquery' %}
            SAFE_DIVIDE(sum(case when score_album >= 90 then 1 else 0 end) * 100.0, count(score_album))
        {% else %}
            case
                when count(score_album) = 0 then null
                else sum(case when score_album >= 90 then 1 else 0 end) * 100.0 / count(score_album)
            end
        {% endif %} as pct_excellent,
        {% if target.type == 'bigquery' %}
            SAFE_DIVIDE(sum(case when score_album >= 80 then 1 else 0 end) * 100.0, count(score_album))
        {% else %}
            case
                when count(score_album) = 0 then null
                else sum(case when score_album >= 80 then 1 else 0 end) * 100.0 / count(score_album)
            end
        {% endif %} as pct_high_quality,
        min(formed_year) as earliest_band,
        max(formed_year) as latest_band,
        sum(is_active_flag) as active_bands,
        sum(is_veteran_band) as veteran_bands,
        count(distinct country) as countries_count
    from base
    group by death_metal_subgenre, continent
),

medians as (
    select
        death_metal_subgenre,
        continent,
        {% if target.type == 'bigquery' %}
            percentile_cont(score_album, 0.5) over(partition by death_metal_subgenre, continent) as median_score
        {% else %}
            percentile_cont(0.5) within group (order by score_album) as median_score
        {% endif %}
    from base
    where score_album is not null
    {% if target.type != 'bigquery' %}
    group by death_metal_subgenre, continent
    {% endif %}
),

sub_decade_count as (
    select
        death_metal_subgenre,
        continent,
        release_decade,
        count(*) as cnt
    from base
    group by death_metal_subgenre, continent, release_decade
),

dominant_decade_cte as (
    select
        death_metal_subgenre,
        continent,
        release_decade as dominant_decade
    from (
        select *, row_number() over(
            partition by death_metal_subgenre, continent
            order by cnt desc, release_decade
        ) as rn
        from sub_decade_count
    ) where rn = 1
),

sub_country_count as (
    select
        death_metal_subgenre,
        continent,
        country,
        count(*) as cnt
    from base
    group by death_metal_subgenre, continent, country
),

dominant_country_cte as (
    select
        death_metal_subgenre,
        continent,
        country as dominant_country
    from (
        select *, row_number() over(
            partition by death_metal_subgenre, continent
            order by cnt desc, country
        ) as rn
        from sub_country_count
    ) where rn = 1
),

final as (
    select
        a.*,
        m.median_score,
        d_dec.dominant_decade,
        d_ctry.dominant_country
    from aggregated a
    left join medians m using (death_metal_subgenre, continent)
    left join dominant_decade_cte d_dec using(death_metal_subgenre, continent)
    left join dominant_country_cte d_ctry using(death_metal_subgenre, continent)
)

select
    *,
    {{ get_row_number(order_by='avg_score desc') }} as quality_rank,
    {{ get_row_number(order_by='total_bands desc') }} as popularity_rank,
    {{ get_row_number(order_by='pct_excellent desc') }} as excellence_rank
from final