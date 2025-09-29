-- Staging model for Yellow Taxi trips
-- This pulls from the cleaned DuckDB table and tags rows with vehicle_type

select
    *,
    'yellow_taxi' as vehicle_type
from {{ source('main', 'yellow_clean') }}

