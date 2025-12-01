
-- xxx in a table `mart_weather_weekly.sql` we want to see **all** weather stats from the `prep_weather_daily` model aggregated weekly. xxx ---
with daily_data as (
    select
        date,
        airport_code,
        avg_temp_c,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        date_part('day', date) as date_day,
        date_part('month', date) as date_month,
        date_part('year', date) as date_year,
        date_part('week', date) as cw,
        to_char(date, 'fmmonth') as month_name,
        case
            when to_char(date, 'fmmonth') in ('december', 'january', 'february') then 'winter'
            when to_char(date, 'fmmonth') in ('march', 'april', 'may') then 'spring'
            when to_char(date, 'fmmonth') in ('june', 'july', 'august') then 'summer'
            when to_char(date, 'fmmonth') in ('september', 'october', 'november') then 'autumn'
        end as season
    from {{ ref('prep_weather_daily') }}
),
weekly_aggregation as (
    select
        date_year,
        cw,
        airport_code,
        season,
        avg(avg_temp_c) as avg_temp_c,
        avg(min_temp_c) as avg_min_temp_c,
        avg(max_temp_c) as avg_max_temp_c,
        avg(precipitation_mm) as avg_precipitation_mm,
        avg(max_snow_mm) as avg_max_snow_mm
    from daily_data
    group by date_year, cw, airport_code, season
)
select
    date_year,
    cw,
    airport_code,
    season,
    round(avg_temp_c, 2) as avg_temp_c,
    round(avg_min_temp_c, 2) as avg_min_temp_c,
    round(avg_max_temp_c, 2) as avg_max_temp_c,
    round(avg_precipitation_mm, 2) as avg_precipitation_mm,
    round(avg_max_snow_mm, 2) as avg_max_snow_mm
from weekly_aggregation
order by date_year, cw, airport_code

