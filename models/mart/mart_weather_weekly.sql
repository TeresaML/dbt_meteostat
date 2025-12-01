-- XXX In a table `mart_weather_weekly.sql` we want to see **all** weather stats from the `prep_weather_daily` model aggregated weekly. XXX ---

SELECT * FROM staging_weather_daily;


WITH daily_data AS (
    SELECT
        *, 
        DATE_PART('day', date) AS date_day,          -- number of the day of month
        DATE_PART('month', date) AS date_month,      -- number of the month of year
        DATE_PART('year', date) AS date_year,        -- number of year
        DATE_PART('week', date) AS cw,               -- number of the week of year
        TO_CHAR(date, 'FMmonth') AS month_name,      -- name of the month
        TO_CHAR(date, 'FMday') AS weekday,           -- name of the weekday
        CASE
            WHEN TO_CHAR(date, 'FMmonth') IN ('december', 'january', 'february') THEN 'winter'
            WHEN TO_CHAR(date, 'FMmonth') IN ('march', 'april', 'may') THEN 'spring'
            WHEN TO_CHAR(date, 'FMmonth') IN ('june', 'july', 'august') THEN 'summer'
            WHEN TO_CHAR(date, 'FMmonth') IN ('september', 'october', 'november') THEN 'autumn'
        END AS season
    FROM staging_weather_daily
    GROUP BY date_year, cw, airport_code, season
)
SELECT *
FROM daily_data
ORDER BY date;


AVG('avg_temp_c', 'avg_min_temo_c', 'avg_max_temp_c'_