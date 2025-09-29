# NOTE:
# This file is intentionally left minimal because all required transformations
# (trip_co2_kgs, avg_mph, hour_of_day, day_of_week, week_of_year, month_of_year)
# have been implemented using dbt models in dbt/models/.
# See: models/transforms/yellow_transformed.sql and green_transformed.sql

import duckdb
import logging

# Configure logging for the transformation step
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="transform.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "emissions.duckdb"

def transform_table(con, clean_table, transformed_table, pickup_col, dropoff_col, vehicle_type):
    """Transform a cleaned trips table into a transformed table by adding
    CO2, avg_mph, and datetime breakdown columns."""
    logger.info(f"Transforming {clean_table} -> {transformed_table}")

    # Drop transformed table if it already exists
    con.execute(f"DROP TABLE IF EXISTS {transformed_table}")

    # Trip duration in seconds
    duration_expr = f"date_diff('second', {pickup_col}, {dropoff_col})"

    # Create transformed table with new calculated columns
    con.execute(f"""
        CREATE TABLE {transformed_table} AS
        SELECT t.*,

               -- Lookup CO₂ factor per vehicle type from vehicle_emissions table
               (t.trip_distance *
                 (SELECT co2_grams_per_mile
                  FROM vehicle_emissions
                  WHERE vehicle_type = '{vehicle_type}') / 1000.0) AS trip_co2_kgs,

               -- Average mph (distance / hours)
               CASE WHEN {duration_expr} > 0
                    THEN t.trip_distance / ({duration_expr} / 3600.0)
                    ELSE NULL END AS avg_mph,

               -- Extract datetime parts
               date_part('hour', {pickup_col})   AS hour_of_day,
               date_part('dow', {pickup_col})    AS day_of_week,
               (date_part('week', {pickup_col}) - 1)   AS week_of_year, -- Weeks 0–52
               date_part('month', {pickup_col})  AS month_of_year

        FROM {clean_table} t
    """)

    # Log number of rows created
    count = con.execute(f"SELECT COUNT(*) FROM {transformed_table}").fetchone()[0]
    logger.info(f"{transformed_table} created with {count} rows")
    print(f"{transformed_table} created with {count} rows")

def main():
    """Main function: connect to DuckDB and run transformations for both Yellow and Green taxi data."""
    con = duckdb.connect(DB_PATH, read_only=False)

    # Transform Yellow taxi trips (tpep timestamps)
    transform_table(con,
                    clean_table="yellow_clean",
                    transformed_table="yellow_transformed",
                    pickup_col="tpep_pickup_datetime",
                    dropoff_col="tpep_dropoff_datetime",
                    vehicle_type="yellow_taxi")

    # Transform Green taxi trips (lpep timestamps)
    transform_table(con,
                    clean_table="green_clean",
                    transformed_table="green_transformed",
                    pickup_col="lpep_pickup_datetime",
                    dropoff_col="lpep_dropoff_datetime",
                    vehicle_type="green_taxi")

    con.close()

if __name__ == "__main__":  # Script entry point
    main()

