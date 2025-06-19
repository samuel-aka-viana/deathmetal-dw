{{ config(materialized='table') }}

with source_data as (
    select
        band_id,
        band_name,
        country,
        status,
        formed_year,
        genre,
        theme,
        active_periods
    from {{ ref('stg_metal_bands') }}
),

parsed_active as (
    select
        *,
        case
            when active_periods = 'N/A' or active_periods is null then 'N/A'
            else active_periods
        end as clean_active,

        case
            when active_periods = 'N/A' or active_periods is null then null
            else trim({{ split_part('active_periods', '|', 1) }})
        end as period_1,

        case
            when active_periods = 'N/A' or active_periods is null then null
            else trim({{ split_part('active_periods', '|', 2) }})
        end as period_2,

        case
            when active_periods = 'N/A' or active_periods is null then null
            else trim({{ split_part('active_periods', '|', 3) }})
        end as period_3,

        case
            when active_periods = 'N/A' or active_periods is null then 0
            when active_periods like '%|%|%' then 3
            when active_periods like '%|%' then 2
            else 1
        end as periods_count,

        case
            when active_periods = 'N/A' or active_periods is null then false
            when active_periods like '%(as %' then true
            else false
        end as has_name_change

    from source_data
),

periods_with_last as (
    select
        *,
        case
            when clean_active = 'N/A' then null
            when periods_count = 3 then period_3
            when periods_count = 2 then period_2
            else period_1
        end as last_period

    from parsed_active
),

simple_parsing as (
    select
        *,
        case
            when clean_active = 'N/A' then null
            when period_1 is null then null
            when period_1 like '____-%' or period_1 like '____-present' then
                {% if target.type == 'bigquery' %}
                    SAFE_CAST(SUBSTR(period_1, 1, 4) AS INT64)
                {% else %}
                    cast(left(period_1, 4) as integer)
                {% endif %}
            when period_1 like '?-%' and period_2 is not null and period_2 like '____-%' then
                {% if target.type == 'bigquery' %}
                    SAFE_CAST(SUBSTR(period_2, 1, 4) AS INT64)
                {% else %}
                    cast(left(period_2, 4) as integer)
                {% endif %}
            when period_1 like '?-%' and length(period_1) = 6 then
                {% if target.type == 'bigquery' %}
                    SAFE_CAST(SUBSTR(period_1, -4) AS INT64)
                {% else %}
                    cast(right(period_1, 4) as integer)
                {% endif %}
            else null
        end as start_year,

        case
            when clean_active = 'N/A' then null
            when periods_count >= 1 and last_period is not null then
                case
                    when last_period like '%present%' then null
                    when last_period like '%?%' then null
                    when last_period like '____-____' then
                        {% if target.type == 'bigquery' %}
                            SAFE_CAST(SUBSTR(last_period, -4) AS INT64)
                        {% else %}
                            cast(right(last_period, 4) as integer)
                        {% endif %}
                    when last_period like '?-____' then
                        {% if target.type == 'bigquery' %}
                            SAFE_CAST(SUBSTR(last_period, -4) AS INT64)
                        {% else %}
                            cast(right(last_period, 4) as integer)
                        {% endif %}
                    else null
                end
            else null
        end as end_year

    from periods_with_last
)

select
    band_id,
    band_name,
    country,
    status,
    formed_year,
    genre,
    theme,
    active_periods as original_active,

    case
        when clean_active = 'N/A' then 'Unknown'
        when last_period like '%present%' then 'Active'
        when last_period like '%?%' then 'Unknown'
        when last_period like '____-____' then 'Split-up'
        else 'Unknown'
    end as current_status,

    start_year,
    end_year,

    case
        when start_year is null or clean_active = 'N/A' then null
        when last_period like '%present%' and start_year is not null then 2024 - start_year
        when end_year is not null and start_year is not null then end_year - start_year
        else null
    end as years_active,

    has_name_change,

    case
        when not has_name_change then null
        when clean_active = 'N/A' then null
        when active_periods like '%(as %' then
            {% if target.type == 'bigquery' %}
                TRIM(SPLIT(SPLIT(active_periods, '(as ')[OFFSET(1)], ')')[OFFSET(0)])
            {% else %}
                split_part(split_part(active_periods, '(as ', 2), ')', 1)
            {% endif %}
        else null
    end as previous_name,

    case
        when not has_name_change then null
        when clean_active = 'N/A' then null
        when period_2 is not null and period_2 like '____-%' then
            {% if target.type == 'bigquery' %}
                SAFE_CAST(SUBSTR(period_2, 1, 4) AS INT64)
            {% else %}
                cast(left(period_2, 4) as integer)
            {% endif %}
        else null
    end as name_change_year,

    periods_count,
    case when active_periods = 'N/A' then true else false end as is_na_case

from simple_parsing
order by band_name