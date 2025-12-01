


WITH origin_info AS (
    SELECT DISTINCT ON (origin) origin, city, country, airport_name
    FROM {{ ref('joined_table') }}
),
dest_info AS (
    SELECT DISTINCT ON (dest) dest, city, country, airport_name
    FROM {{ ref('joined_table') }}
)
SELECT
    o.origin AS origin_airport,
    o.dest AS dest_airport,
    COUNT(*) AS total_flights_this_route,
    SUM(CASE WHEN o.cancelled = 1 THEN 1 ELSE 0 END) AS total_flights_cancelled,
    SUM(CASE WHEN o.diverted = 1 THEN 1 ELSE 0 END) AS total_flights_diverted,
    COUNT(DISTINCT o."tail_number") AS unique_airplanes,
    COUNT(DISTINCT o."airline") AS unique_airlines,
    AVG(o.actual_elapsed_time) AS avg_el_time,
    AVG(o.arr_delay) AS avg_arr_delay,
    MAX(o.arr_delay) AS max_arr_delay,
    MIN(o.arr_delay) AS min_arr_delay,
    oi.city AS origin_city,
    oi.country AS origin_country,
    oi.airport_name AS origin_airport_name,
    di.city AS dest_city,
    di.country AS dest_country,
    di.airport_name AS dest_airport_name
FROM
    {{ ref('joined_table') }} o
LEFT JOIN origin_info oi ON o.origin = oi.origin
LEFT JOIN dest_info di ON o.dest = di.dest
GROUP BY
    o.origin, o.dest, oi.city, oi.country, oi.airport_name, di.city, di.country, di.airport_name
ORDER BY
    o.origin, o.dest


