{{ config(materialized='table') }}

select
    s.*,

    -- CO₂ per trip in kilograms
    (s.trip_distance * ve.co2_grams_per_mile / 1000.0) as trip_co2_kgs,

    -- Average miles per hour
    case when date_diff('second', s.lpep_pickup_datetime, s.lpep_dropoff_datetime) > 0
         then s.trip_distance / (date_diff('second', s.lpep_pickup_datetime, s.lpep_dropoff_datetime) / 3600.0)
    end as avg_mph,

    -- Datetime parts
    date_part('hour', s.lpep_pickup_datetime) as hour_of_day,
    date_part('dow', s.lpep_pickup_datetime)  as day_of_week,
    date_part('week', s.lpep_pickup_datetime) as week_of_year,
    date_part('month', s.lpep_pickup_datetime) as month_of_year

from {{ ref('green_staging') }} s
join {{ source('main', 'vehicle_emissions') }} ve
  on ve.vehicle_type = 'green_taxi'

