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