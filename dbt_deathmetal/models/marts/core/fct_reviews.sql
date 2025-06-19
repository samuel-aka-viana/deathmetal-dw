{{ config(materialized='view') }}

with reviews_base as (
    select
        r.review_id,
        r.album_id,
        r.review_title,
        r.score_album,
        r.content_review,

        a.band_id,
        a.album_title,
        a.album_year,
        a.release_decade,
        a.release_era,
        a.band_career_phase,
        a.years_since_formation,
        a.is_debut_album,
        a.is_golden_age_release,
        a.album_number_in_discography,

        a.band_name,
        a.country,
        a.continent,
        a.genre,
        a.death_metal_subgenre,
        a.band_status,
        a.formation_era,
        a.band_maturity,

        case
            when r.score_album >= 95 then 'Masterpiece (95-100)'
            when r.score_album >= 90 then 'Excellent (90-94)'
            when r.score_album >= 80 then 'Very Good (80-89)'
            when r.score_album >= 70 then 'Good (70-79)'
            when r.score_album >= 60 then 'Average (60-69)'
            when r.score_album >= 50 then 'Below Average (50-59)'
            when r.score_album >= 40 then 'Poor (40-49)'
            else 'Very Poor (0-39)'
        end as score_category,

        case
            when r.score_album >= 80 then 'High (80+)'
            when r.score_album >= 60 then 'Medium (60-79)'
            else 'Low (<60)'
        end as score_range,

        {% if target.type == 'bigquery' %}
            LENGTH(r.review_title)
        {% else %}
            length(r.review_title)
        {% endif %} as review_title_length,

        case
            when r.review_title is null or trim(r.review_title) = '' then 'No Title'
            when {% if target.type == 'bigquery' %}LENGTH(r.review_title){% else %}length(r.review_title){% endif %} <= 20 then 'Short Title'
            when {% if target.type == 'bigquery' %}LENGTH(r.review_title){% else %}length(r.review_title){% endif %} <= 50 then 'Medium Title'
            else 'Long Title'
        end as review_title_category,

        {% if target.type == 'bigquery' %}
            LENGTH(r.content_review)
        {% else %}
            length(r.content_review)
        {% endif %} as review_content_length,

        case
            when r.content_review is null or trim(r.content_review) = '' then 'No Content'
            when {% if target.type == 'bigquery' %}LENGTH(r.content_review){% else %}length(r.content_review){% endif %} <= 100 then 'Brief Review'
            when {% if target.type == 'bigquery' %}LENGTH(r.content_review){% else %}length(r.content_review){% endif %} <= 500 then 'Short Review'
            when {% if target.type == 'bigquery' %}LENGTH(r.content_review){% else %}length(r.content_review){% endif %} <= 1000 then 'Medium Review'
            when {% if target.type == 'bigquery' %}LENGTH(r.content_review){% else %}length(r.content_review){% endif %} <= 2000 then 'Long Review'
            else 'Very Long Review'
        end as review_content_category,

        case when r.review_title is not null and trim(r.review_title) != '' then 1 else 0 end as has_review_title,
        case when r.content_review is not null and trim(r.content_review) != '' then 1 else 0 end as has_review_content,
        case when {% if target.type == 'bigquery' %}LENGTH(r.content_review){% else %}length(r.content_review){% endif %} >= 100 then 1 else 0 end as is_substantial_review,

        case when r.score_album >= 90 then 1 else 0 end as is_excellent_score,
        case when r.score_album >= 80 then 1 else 0 end as is_high_score,
        case when r.score_album >= 70 then 1 else 0 end as is_good_score,
        case when r.score_album < 50 then 1 else 0 end as is_poor_score,

        case when r.score_album > 75 then r.score_album - 75 else 0 end as score_above_threshold,
        {% if target.type == 'bigquery' %}
            ABS(r.score_album - 70)
        {% else %}
            abs(r.score_album - 70)
        {% endif %} as score_deviation_from_average

    from {{ ref('stg_metal_reviews') }} r
    join {{ ref('dim_albums') }} a on r.album_id = a.album_id
    where r.score_album is not null
),

reviews_with_rankings as (
    select
        *,
        row_number() over (order by score_album desc) as overall_score_rank,
        row_number() over (partition by band_id order by score_album desc) as band_score_rank,
        row_number() over (partition by country order by score_album desc) as country_score_rank,
        row_number() over (partition by release_decade order by score_album desc) as decade_score_rank,
        row_number() over (partition by death_metal_subgenre order by score_album desc) as subgenre_score_rank,

        {% if target.type == 'bigquery' %}
            PERCENT_RANK() OVER (ORDER BY score_album)
        {% else %}
            percent_rank() over (order by score_album)
        {% endif %} as score_percentile,

        {% if target.type == 'bigquery' %}
            PERCENT_RANK() OVER (PARTITION BY release_decade ORDER BY score_album)
        {% else %}
            percent_rank() over (partition by release_decade order by score_album)
        {% endif %} as decade_score_percentile,

        {% if target.type == 'bigquery' %}
            PERCENT_RANK() OVER (PARTITION BY death_metal_subgenre ORDER BY score_album)
        {% else %}
            percent_rank() over (partition by death_metal_subgenre order by score_album)
        {% endif %} as subgenre_score_percentile,

        score_album - avg(score_album) over () as score_vs_overall_avg,
        score_album - avg(score_album) over (partition by band_id) as score_vs_band_avg,
        score_album - avg(score_album) over (partition by country) as score_vs_country_avg,
        score_album - avg(score_album) over (partition by release_decade) as score_vs_decade_avg,

        case when score_album > avg(score_album) over () then 1 else 0 end as is_above_overall_avg,
        case when score_album > avg(score_album) over (partition by band_id) then 1 else 0 end as is_above_band_avg,
        case when score_album > avg(score_album) over (partition by death_metal_subgenre) then 1 else 0 end as is_above_subgenre_avg,

        count(*) over (partition by band_id) as band_total_reviews,
        count(*) over (partition by album_id) as album_total_reviews

    from reviews_base
)

select
    review_id,
    album_id,
    band_id,
    review_title,
    score_album,
    content_review,
    score_category,
    score_range,
    review_title_length,
    review_title_category,
    review_content_length,
    review_content_category,
    album_title,
    band_name,
    album_year,
    country,
    continent,
    genre,
    death_metal_subgenre,
    release_decade,
    release_era,
    formation_era,
    band_career_phase,
    band_maturity,
    years_since_formation,
    album_number_in_discography,
    has_review_title,
    has_review_content,
    is_substantial_review,
    is_excellent_score,
    is_high_score,
    is_good_score,
    is_poor_score,
    is_debut_album,
    is_golden_age_release,
    is_above_overall_avg,
    is_above_band_avg,
    is_above_subgenre_avg,
    overall_score_rank,
    band_score_rank,
    country_score_rank,
    decade_score_rank,
    subgenre_score_rank,
    score_percentile,
    decade_score_percentile,
    subgenre_score_percentile,
    score_vs_overall_avg,
    score_vs_band_avg,
    score_vs_country_avg,
    score_vs_decade_avg,
    score_above_threshold,
    score_deviation_from_average,
    band_total_reviews,
    album_total_reviews,
    {{ current_timestamp_func() }} as created_at,
    {{ current_timestamp_func() }} as updated_at

from reviews_with_rankings