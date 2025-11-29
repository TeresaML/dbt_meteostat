{{
  config(
    materialized='joined_table'
  )
}}
SELECT
    a.faa,
    a.name AS airport_name,
    a.city,
    a.country,
    f.*
FROM
    {{ ref('prep_airports') }} a
LEFT JOIN
    {{ ref('prep_flights') }} f ON a.faa = f.origin


WITH stats AS (
    SELECT
        faa,
        -- 1. unique number of departures connections (Abflüge)
        COUNT(DISTINCT CASE WHEN origin = faa THEN dest END) AS unique_departures_connections,
        -- 2. unique number of arrival connections (Ankünfte)
        COUNT(DISTINCT CASE WHEN dest = faa THEN origin END) AS unique_arrivals_connections,
        -- 3. how many flights were planned in total (departures & arrivals)
        COUNT(*) AS total_flights_planned,
        -- 4. how many flights were canceled in total (departures & arrivals)
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_flights_cancelled,
        -- 5. how many flights were diverted in total (departures & arrivals)
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_flights_diverted,
        -- 6. how many flights actually occurred in total (departures & arrivals)
        SUM(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE 0 END) AS total_flights_occurred
      FROM
        {{ ref('joined_table') }}
    GROUP BY
        faa
)
SELECT * FROM stats;