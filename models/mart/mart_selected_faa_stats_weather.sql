
WITH stats AS (
select 
	faa,
	COUNT(DISTINCT CASE WHEN origin = faa THEN dest END) AS unique_departures_connections,
	COUNT(DISTINCT CASE WHEN dest = faa THEN origin END) AS unique_arrivals_connections,
	count (*) as total_flights_planned,
	sum (case when cancelled = 1 then 1 else 0 end) as total_number_flights_cancelled,
	sum (case when diverted = 1 then 1 else 0 end) as total_number_flights_diverted,
	sum (case when arr_time is not null then 1 else 0 end ) as total_number_flights
FROM
        {{ ref('joined_table') }}
where faa in  ('LAX', 'MIA', 'JFK')
group by faa
)
SELECT * FROM stats
