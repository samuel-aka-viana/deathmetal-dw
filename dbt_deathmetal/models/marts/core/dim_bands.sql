{{ config(materialized='table') }}

with bands_enriched as (
    select
        band_id,
        band_name,
        country,
        status,
        formed_year,
        genre,
        theme,
        original_active,
        current_status,
        start_year,
        end_year,
        years_active,
        has_name_change,
        previous_name,
        name_change_year,
        periods_count,
        is_na_case,


        case
            when years_active >= 30 then 'Veteran (30+ years)'
            when years_active >= 15 then 'Established (15-29 years)'
            when years_active >= 5 then 'Emerging (5-14 years)'
            when years_active >= 1 then 'New (1-4 years)'
            else 'Unknown Duration'
        end as band_maturity,


        case
            when formed_year between 1960 and 1979 then 'Proto-Metal Era'
            when formed_year between 1980 and 1989 then 'Classic Metal Era'
            when formed_year between 1990 and 1999 then 'Death Metal Golden Age'
            when formed_year between 2000 and 2009 then 'Nu-Metal/Mainstream Era'
            when formed_year between 2010 and 2019 then 'Revival Era'
            when formed_year >= 2020 then 'Modern Era'
            else 'Unknown Era'
        end as formation_era,


        case
            when formed_year between 1980 and 1989 then '1980s'
            when formed_year between 1990 and 1999 then '1990s'
            when formed_year between 2000 and 2009 then '2000s'
            when formed_year between 2010 and 2019 then '2010s'
            when formed_year >= 2020 then '2020s+'
            else 'Pre-1980s'
        end as formation_decade,


        case
            when country in ('United States', 'Canada', 'Mexico') then 'North America'
            when country in ('Brazil', 'Argentina', 'Chile', 'Colombia', 'Peru', 'Ecuador', 'Venezuela', 'Uruguay', 'Paraguay', 'Bolivia') then 'South America'
            when country in ('Germany', 'United Kingdom', 'France', 'Italy', 'Spain', 'Netherlands', 'Belgium', 'Switzerland', 'Austria', 'Sweden', 'Norway', 'Finland', 'Denmark', 'Poland', 'Czech Republic', 'Hungary', 'Romania', 'Bulgaria', 'Croatia', 'Serbia', 'Greece', 'Portugal', 'Ireland', 'Slovakia', 'Slovenia', 'Estonia', 'Latvia', 'Lithuania', 'Malta', 'Cyprus', 'Luxembourg', 'Monaco', 'Liechtenstein', 'Andorra', 'San Marino', 'Vatican City', 'Russia', 'Ukraine', 'Belarus', 'Moldova') then 'Europe'
            when country in ('Japan', 'South Korea', 'China', 'India', 'Thailand', 'Malaysia', 'Singapore', 'Indonesia', 'Philippines', 'Vietnam', 'Taiwan', 'Hong Kong', 'Mongolia', 'Kazakhstan', 'Kyrgyzstan', 'Tajikistan', 'Turkmenistan', 'Uzbekistan', 'Afghanistan', 'Pakistan', 'Bangladesh', 'Sri Lanka', 'Nepal', 'Bhutan', 'Myanmar', 'Cambodia', 'Laos', 'Brunei', 'Maldives') then 'Asia'
            when country in ('Australia', 'New Zealand', 'Papua New Guinea', 'Fiji', 'Solomon Islands', 'Vanuatu', 'Samoa', 'Tonga', 'Kiribati', 'Tuvalu', 'Nauru', 'Palau', 'Marshall Islands', 'Micronesia') then 'Oceania'
            when country in ('Egypt', 'South Africa', 'Nigeria', 'Kenya', 'Morocco', 'Algeria', 'Tunisia', 'Libya', 'Ghana', 'Ethiopia', 'Tanzania', 'Uganda', 'Madagascar', 'Cameroon', 'Angola', 'Mozambique', 'Zambia', 'Zimbabwe', 'Botswana', 'Namibia', 'Mauritius', 'Seychelles') then 'Africa'
            else 'Other/Unknown'
        end as continent,


        case when current_status = 'Active' then 1 else 0 end as is_active_flag,
        case when has_name_change then 1 else 0 end as has_name_change_flag,
        case when formed_year between 1990 and 1999 then 1 else 0 end as is_golden_age_band,
        case when years_active >= 20 then 1 else 0 end as is_veteran_band,
        case when genre like '%Death Metal%' then 1 else 0 end as is_pure_death_metal,


        case when is_na_case then 'Insufficient Data' else 'Has Data' end as data_quality,


        case
            when current_status = 'Active' then 2024 - formed_year
            when end_year is not null then end_year - formed_year
            else null
        end as total_career_span,


        case
            when genre ilike '%brutal%' then 'Brutal Death Metal'
            when genre ilike '%technical%' or genre ilike '%tech%' then 'Technical Death Metal'
            when genre ilike '%melodic%' then 'Melodic Death Metal'
            when genre ilike '%progressive%' then 'Progressive Death Metal'
            when genre ilike '%blackened%' or genre ilike '%black%' then 'Blackened Death Metal'
            when genre ilike '%doom%' then 'Death/Doom Metal'
            when genre ilike '%grind%' then 'Deathgrind'
            when genre = 'Death Metal' then 'Traditional Death Metal'
            else 'Other Death Metal'
        end as death_metal_subgenre

    from {{ ref('int_metal_bands_parsed') }}
)

select
    band_id,

    band_name,
    country,
    continent,
    status,
    current_status,
    formed_year,
    start_year,
    end_year,
    years_active,
    total_career_span,

    genre,
    death_metal_subgenre,
    theme,

    has_name_change,
    has_name_change_flag,
    previous_name,
    name_change_year,
    periods_count,

    formation_era,
    formation_decade,
    band_maturity,

    is_active_flag,
    is_golden_age_band,
    is_veteran_band,
    is_pure_death_metal,

    data_quality,
    is_na_case,
    original_active,

    current_timestamp as created_at,
    current_timestamp as updated_at,
    true as is_current_record

from bands_enriched