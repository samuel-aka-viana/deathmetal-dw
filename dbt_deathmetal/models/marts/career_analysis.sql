{{ config(materialized='view') }}

with career_patterns as (
    select
        a.band_id,
        a.band_name,
        a.band_career_phase,
        a.album_number_in_discography,
        a.years_since_formation,
        a.album_title,
        a.album_year,
        r.score_album,
        case
            when a.album_number_in_discography = 1 then 'Debut'
            when a.album_number_in_discography = 2 then 'Sophomore'
            when a.album_number_in_discography = 3 then 'Third'
            when a.album_number_in_discography <= 5 then 'Early Career'
            else 'Mature Career'
        end as album_career_position,
        lag(r.score_album, 1) over (partition by a.band_id order by a.album_year) as previous_album_score,
        r.score_album - lag(r.score_album, 1) over (partition by a.band_id order by a.album_year) as score_change,
        avg(r.score_album) over (partition by a.band_id) as band_avg_score,
        r.score_album - avg(r.score_album) over (partition by a.band_id) as score_vs_band_avg,
        case when a.years_since_formation <= 2 then 'Immediate'
             when a.years_since_formation <= 5 then 'Early'
             when a.years_since_formation <= 10 then 'Mid'
             else 'Late'
        end as career_timing,
        case when a.album_number_in_discography = 2 and
                  r.score_album < lag(r.score_album, 1) over (partition by a.band_id order by a.album_year)
             then 1 else 0 end as has_sophomore_slump,
        case when r.score_album = max(r.score_album) over (partition by a.band_id) then 1 else 0 end as is_band_best_album
    from {{ ref('dim_albums') }} a
    join {{ ref('fct_reviews') }} r on a.album_id = r.album_id
    where r.score_album is not null
)

select * from career_patterns