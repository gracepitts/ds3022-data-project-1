-- Staging model for Green Taxi trips
-- This pulls from the cleaned DuckDB table and tags rows with vehicle_type

select
    *,
    'green_taxi' as vehicle_type
from {{ source('main', 'green_clean') }}
