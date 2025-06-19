{{ config(materialized='view') }}

with base as (
    select
        b.country,
        b.continent,
        b.band_id,
        b.formed_year,
        b.formation_era,
        b.death_metal_subgenre,
        b.is_active_flag,
        b.is_veteran_band,
        a.album_id,
        a.release_decade
    from {{ ref('dim_bands') }} b
    left join {{ ref('dim_albums') }} a
      on b.band_id = a.band_id
),

aggregated as (
    select
        country,
        continent,

        count(distinct band_id) as total_bands,
        count(distinct album_id) as total_albums,

        min(formed_year) as first_band_formed,
        max(formed_year) as latest_band_formed,

        sum(is_active_flag) as active_bands,
        sum(is_veteran_band) as veteran_bands,

        count(distinct death_metal_subgenre) as subgenres_count,

        sum(case when release_decade = '1990s' then 1 else 0 end) as albums_90s,
        sum(case when release_decade = '2000s' then 1 else 0 end) as albums_00s,
        sum(case when release_decade = '2010s' then 1 else 0 end) as albums_10s,

        {% if target.type == 'bigquery' %}
            SAFE_DIVIDE(count(distinct album_id), count(distinct band_id))
        {% else %}
            count(distinct album_id) * 1.0 / nullif(count(distinct band_id), 0)
        {% endif %} as albums_per_band
    from base
    group by country, continent
),

era_counts as (
    select
        country,
        continent,
        formation_era,
        count(*) as cnt
    from base
    where formation_era is not null
    group by country, continent, formation_era
),

dominant_era_cte as (
    select
        country,
        continent,
        formation_era as dominant_era
    from (
        select
            country,
            continent,
            formation_era,
            row_number() over (
                partition by country, continent
                order by cnt desc, formation_era
            ) as rn
        from era_counts
    ) where rn = 1
),

subgenre_counts as (
    select
        country,
        continent,
        death_metal_subgenre,
        count(*) as cnt
    from base
    where death_metal_subgenre is not null
    group by country, continent, death_metal_subgenre
),

dominant_subgenre_cte as (
    select
        country,
        continent,
        death_metal_subgenre as dominant_subgenre
    from (
        select
            country,
            continent,
            death_metal_subgenre,
            row_number() over (
                partition by country, continent
                order by cnt desc, death_metal_subgenre
            ) as rn
        from subgenre_counts
    ) where rn = 1
),

final as (
    select
        a.*,
        d_era.dominant_era,
        d_sub.dominant_subgenre,
        row_number() over (order by a.total_bands desc) as country_quantity_rank
    from aggregated a
    left join dominant_era_cte d_era
      using (country, continent)
    left join dominant_subgenre_cte d_sub
      using (country, continent)
)

select * from final
