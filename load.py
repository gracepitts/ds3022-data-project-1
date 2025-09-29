import duckdb
import os
import logging
import glob

# Configure logging to record process details into load.log
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DATA_PATH = "data"  # Folder where taxi parquet files and CSV live

def load_parquet_files():

    con = None

    try:
        # Connect to a local DuckDB instance (creates emissions.duckdb if it doesn't exist)
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # Drop old tables if they already exist to avoid conflicts
        con.execute(f"""
            DROP TABLE IF EXISTS yellow_trips_2024;
            DROP TABLE IF EXISTS green_trips_2024;
            DROP TABLE IF EXISTS vehicle_emissions;
            """)
        logger.info("Dropped table if exists")
        
        # Collect all parquet files for 2024 taxi data
        yellow_files = sorted(glob.glob(os.path.join(DATA_PATH, "yellow_tripdata_2024-*.parquet")))
        green_files = sorted(glob.glob(os.path.join(DATA_PATH, "green_tripdata_2024-*.parquet")))

        # Error handling if files are missing
        if not yellow_files or not green_files:
            raise FileNotFoundError("Yellow or Green taxi parquet files for 2024 not found")

        # Create Yellow trips table schema using the first file (LIMIT 0 just copies structure)
        con.execute(f"""
            CREATE TABLE yellow_trips_2024 AS 
            SELECT * FROM read_parquet('{yellow_files[0]}') LIMIT 0;
        """)
        logger.info("Created yellow_trips_2024 table")

        # Create Green trips table schema using the first file (structure only)
        con.execute(f"""
            CREATE TABLE green_trips_2024 AS 
            SELECT * FROM read_parquet('{green_files[0]}') LIMIT 0;
        """)
        logger.info("Created green_trips_2024 table")

        # Insert all Yellow taxi parquet files into yellow_trips_2024
        for f in yellow_files:
            con.execute(f"INSERT INTO yellow_trips_2024 SELECT * FROM read_parquet('{f}');")
        logger.info(f"Inserted {len(yellow_files)} yellow taxi files")

        # Insert all Green taxi parquet files into green_trips_2024
        for f in green_files:
            con.execute(f"INSERT INTO green_trips_2024 SELECT * FROM read_parquet('{f}');")
        logger.info(f"Inserted {len(green_files)} green taxi files")

        # Load vehicle emissions CSV as a lookup table
        con.execute(f"""
            CREATE TABLE vehicle_emissions AS 
            SELECT * FROM read_csv_auto('{os.path.join(DATA_PATH, "vehicle_emissions.csv")}', header=True);
        """)
        logger.info("Created vehicle_emissions table")

        # Print and log raw row counts for each table
        yellow_count = con.execute("SELECT COUNT(*) FROM yellow_trips_2024;").fetchone()[0]
        green_count = con.execute("SELECT COUNT(*) FROM green_trips_2024;").fetchone()[0]
        emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions;").fetchone()[0]

        print(f"Yellow trips (raw): {yellow_count}")
        print(f"Green trips (raw): {green_count}")
        print(f"Vehicle emissions rows: {emissions_count}")

        logger.info(f"Yellow trips count: {yellow_count}")
        logger.info(f"Green trips count: {green_count}")
        logger.info(f"Vehicle emissions rows: {emissions_count}")

                # --- Basic descriptive statistics for trips ---
        for table in ["yellow_trips_2024", "green_trips_2024"]:
            print(f"\nBasic stats for {table}:")

            # Calculate basic stats: min/max/avg distance and passenger counts
            stats = con.execute(f"""
                SELECT
                    MIN(trip_distance) AS min_distance,
                    MAX(trip_distance) AS max_distance,
                    AVG(trip_distance) AS avg_distance,
                    MIN(passenger_count) AS min_passengers,
                    MAX(passenger_count) AS max_passengers,
                    AVG(passenger_count) AS avg_passengers
                FROM {table}
            """).fetchdf()

            # Print stats to screen and log them
            print(stats)
            logger.info(f"Basic stats for {table}: {stats.to_dict(orient='records')[0]}")

            # Show earliest and latest pickup times to confirm time coverage
            # Use tpep_* for yellow taxis, lpep_* for green taxis
            pickup_col = "tpep_pickup_datetime" if "yellow" in table else "lpep_pickup_datetime"
            times = con.execute(f"""
                SELECT
                    MIN({pickup_col}) AS earliest_pickup,
                    MAX({pickup_col}) AS latest_pickup
                FROM {table}
            """).fetchdf()

            # Print and log time ranges
            print(times)
            logger.info(f"Pickup time range for {table}: {times.to_dict(orient='records')[0]}")


    except Exception as e:
        # Log and print any errors that occur during loading
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        # Always close the database connection
        if con:
            con.close()
            logger.info("Closed DuckDB connection")


# Entry point: runs load_parquet_files() if script is executed directly
if __name__ == "__main__":
    load_parquet_files()
