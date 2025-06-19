{{ config(materialized='table') }}

with albums_enriched as (
    select
        a.album_id,
        a.band_id,
        a.album_title,
        a.album_year,
        b.band_name,
        b.country,
        b.continent,
        b.genre,
        b.death_metal_subgenre,
        b.formed_year,
        b.current_status as band_status,
        b.formation_era,
        b.formation_decade,
        b.band_maturity,

        case
            when a.album_year between 1980 and 1989 then '1980s'
            when a.album_year between 1990 and 1999 then '1990s'
            when a.album_year between 2000 and 2009 then '2000s'
            when a.album_year between 2010 and 2019 then '2010s'
            when a.album_year >= 2020 then '2020s+'
            else 'Pre-1980s'
        end as release_decade,

        case
            when a.album_year between 1960 and 1979 then 'Proto-Metal Era'
            when a.album_year between 1980 and 1989 then 'Classic Metal Era'
            when a.album_year between 1990 and 1999 then 'Death Metal Golden Age'
            when a.album_year between 2000 and 2009 then 'Nu-Metal/Mainstream Era'
            when a.album_year between 2010 and 2019 then 'Revival Era'
            when a.album_year >= 2020 then 'Modern Era'
            else 'Unknown Era'
        end as release_era,

        case
            when a.album_year = b.formed_year then 'Debut Year'
            when a.album_year - b.formed_year <= 2 then 'Early Career (0-2 years)'
            when a.album_year - b.formed_year <= 5 then 'Establishing Phase (3-5 years)'
            when a.album_year - b.formed_year <= 10 then 'Mature Phase (6-10 years)'
            when a.album_year - b.formed_year <= 20 then 'Veteran Phase (11-20 years)'
            when a.album_year - b.formed_year > 20 then 'Legacy Phase (20+ years)'
            else 'Unknown Phase'
        end as band_career_phase,

        a.album_year - b.formed_year as years_since_formation,

        case when a.album_year = b.formed_year then 1 else 0 end as is_debut_album,
        case when a.album_year between 1990 and 1999 then 1 else 0 end as is_golden_age_release,
        case when a.album_year >= 2020 then 1 else 0 end as is_modern_release,
        case when a.album_year - b.formed_year <= 2 then 1 else 0 end as is_early_career_album,
        case when a.album_year - b.formed_year > 20 then 1 else 0 end as is_legacy_album,


        case
            {% if target.type == 'bigquery' %}
            when LOWER(a.album_title) like '%death%' then 'Death-themed'
            when LOWER(a.album_title) like '%dark%' or LOWER(a.album_title) like '%black%' then 'Dark-themed'
            when LOWER(a.album_title) like '%blood%' or LOWER(a.album_title) like '%gore%' then 'Gore-themed'
            when LOWER(a.album_title) like '%evil%' or LOWER(a.album_title) like '%hell%' or LOWER(a.album_title) like '%demon%' then 'Evil-themed'
            when LOWER(a.album_title) like '%war%' or LOWER(a.album_title) like '%battle%' then 'War-themed'
            when LOWER(a.album_title) like '%ritual%' or LOWER(a.album_title) like '%cult%' then 'Occult-themed'
            {% else %}
            when lower(a.album_title) like '%death%' then 'Death-themed'
            when lower(a.album_title) like '%dark%' or lower(a.album_title) like '%black%' then 'Dark-themed'
            when lower(a.album_title) like '%blood%' or lower(a.album_title) like '%gore%' then 'Gore-themed'
            when lower(a.album_title) like '%evil%' or lower(a.album_title) like '%hell%' or lower(a.album_title) like '%demon%' then 'Evil-themed'
            when lower(a.album_title) like '%war%' or lower(a.album_title) like '%battle%' then 'War-themed'
            when lower(a.album_title) like '%ritual%' or lower(a.album_title) like '%cult%' then 'Occult-themed'
            {% endif %}
            else 'Other-themed'
        end as title_theme_category,


       {% if target.type == 'bigquery' %}
            LENGTH(a.album_title)
        {% else %}
            length(a.album_title)
        {% endif %} as title_length,

        case
            when {% if target.type == 'bigquery' %}LENGTH(a.album_title){% else %}length(a.album_title){% endif %} <= 10 then 'Short (≤10 chars)'
            when {% if target.type == 'bigquery' %}LENGTH(a.album_title){% else %}length(a.album_title){% endif %} <= 20 then 'Medium (11-20 chars)'
            when {% if target.type == 'bigquery' %}LENGTH(a.album_title){% else %}length(a.album_title){% endif %} <= 30 then 'Long (21-30 chars)'
            else 'Very Long (30+ chars)'
        end as title_length_category

    from {{ ref('stg_metal_albums') }} a
    join {{ ref('dim_bands') }} b on a.band_id = b.band_id
    where a.album_year is not null
),

album_rankings as (
    select
        *,
        row_number() over (partition by band_id order by album_year) as album_number_in_discography,
        row_number() over (partition by band_id order by album_year desc) as reverse_album_number,

        row_number() over (partition by release_decade order by album_year) as album_number_in_decade,
        row_number() over (partition by release_era order by album_year) as album_number_in_era,

        row_number() over (partition by country, release_decade order by album_year) as album_number_in_country_decade,

        case when row_number() over (partition by band_id order by album_year) = 1 then 1 else 0 end as is_first_album,
        case when row_number() over (partition by band_id order by album_year desc) = 1 then 1 else 0 end as is_latest_album,
        case when row_number() over (partition by band_id order by album_year) <= 3 then 1 else 0 end as is_early_discography

    from albums_enriched
)

select
    album_id,
    band_id,
    album_title,
    album_year,
    title_length,
    title_length_category,
    title_theme_category,
    band_name,
    country,
    continent,
    genre,
    death_metal_subgenre,
    band_status,
    formed_year,
    release_decade,
    release_era,
    formation_era,
    formation_decade,
    band_maturity,
    band_career_phase,
    years_since_formation,
    album_number_in_discography,
    reverse_album_number,
    album_number_in_decade,
    album_number_in_era,
    album_number_in_country_decade,
    is_debut_album,
    is_first_album,
    is_latest_album,
    is_early_discography,
    is_golden_age_release,
    is_modern_release,
    is_early_career_album,
    is_legacy_album,
    current_timestamp as created_at,
    current_timestamp as updated_at,
    true as is_current_record

from album_rankings