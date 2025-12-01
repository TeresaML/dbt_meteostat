


WITH origin_info AS (
    SELECT DISTINCT ON (origin) origin, city, country, airport_name
    FROM joined_table
),
dest_info AS (
    SELECT DISTINCT ON (dest) dest, city, country, airport_name
    FROM joined_table
)
select
    o.origin as origin_airport,
    o.dest as dest_airport,
    COUNT(*) as total_flights_this_route,
    SUM(CASE WHEN o.cancelled = 1 THEN 1 ELSE 0 END) as total_flights_cancelled,
    SUM(CASE WHEN o.diverted = 1 THEN 1 ELSE 0 END) as total_flights_diverted,
    COUNT(DISTINCT tail_number) as unique_airplanes,
    COUNT(DISTINCT "airline") as unique_airlines,
    AVG(o.actual_elapsed_time) as avg_el_time,
    AVG(o.arr_delay) as avg_arr_delay,
    MAX(o.arr_delay) as max_arr_delay,
    MIN(o.arr_delay) as min_arr_delay,
    oi.city as origin_city,
    oi.country as origin_country,
    oi.airport_name as origin_airport_name,
    di.city as dest_city,
    di.country as dest_country,
    di.airport_name as dest_airport_name
from {{ ref('joined_table') }}
left join  origin_info oi on o.origin = oi.origin
left join dest_info di on o.dest = di.dest
group by o.origin, o.dest, oi.city, oi.country, oi.airport_name, di.city, di.country, di.airport_name
order by o.origin, o.dest

