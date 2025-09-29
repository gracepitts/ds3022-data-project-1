import duckdb
import logging

# Configure logging to track the cleaning process
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
    filename="clean.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "emissions.duckdb"

def get_column_name(con, table, candidates):  # Function: find the correct column name from a list of candidates
    """Return the first matching column name from a list of candidates (handles yellow/green differences)."""
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    cols = [r[1] for r in rows]   # Column names
    logger.info(f"Columns in {table}: {cols}")
    for cand in candidates:
        for col in cols:
            if col.lower() == cand.lower():
                return col
    return None

def clean_table(con, raw_table, clean_table):  # Function: clean a raw taxi trips table and save into a new cleaned table
    logger.info(f"Cleaning {raw_table} -> {clean_table}")

    # Identify pickup/dropoff, passenger, and distance columns
    pickup_col = get_column_name(con, raw_table, ["tpep_pickup_datetime", "lpep_pickup_datetime"])
    dropoff_col = get_column_name(con, raw_table, ["tpep_dropoff_datetime", "lpep_dropoff_datetime"])
    passenger_col = get_column_name(con, raw_table, ["passenger_count"])
    distance_col = get_column_name(con, raw_table, ["trip_distance"])

    if not pickup_col or not dropoff_col or not passenger_col or not distance_col:
        raise ValueError(f"Could not find required columns in {raw_table}")

    logger.info(f"Using columns: pickup={pickup_col}, dropoff={dropoff_col}, "
                f"passenger={passenger_col}, distance={distance_col}")

    # Drop existing cleaned table if it exists
    con.execute(f"DROP TABLE IF EXISTS {clean_table}")

    # Create cleaned table with filtering rules
    con.execute(f"""
        CREATE TABLE {clean_table} AS
        SELECT DISTINCT *  -- Remove duplicates
        FROM {raw_table}
        WHERE {passenger_col} > 0                -- No zero passengers
          AND {distance_col} > 0                 -- No zero distance
          AND {distance_col} <= 100              -- Max 100 miles
          AND date_diff('second', {pickup_col}, {dropoff_col}) <= 86400 -- Max 1 day duration
          AND strftime({pickup_col}, '%Y') = '2024' -- Keep only trips from 2024
    """)

    # Print counts before/after cleaning
    before = con.execute(f"SELECT COUNT(*) FROM {raw_table}").fetchone()[0]
    after = con.execute(f"SELECT COUNT(*) FROM {clean_table}").fetchone()[0]
    logger.info(f"{raw_table}: {before} -> {after} after cleaning")
    print(f"{raw_table}: {before} -> {after} after cleaning")

    # Verification queries
    checks = {
        "Zero passengers": f"{passenger_col} = 0",
        "Zero distance": f"{distance_col} = 0",
        "Too long distance": f"{distance_col} > 100",
        "Too long duration": f"date_diff('second',{pickup_col},{dropoff_col}) > 86400"
    }

    for desc, condition in checks.items():
        q = f"SELECT COUNT(*) FROM {clean_table} WHERE {condition}"
        count = con.execute(q).fetchone()[0]
        logger.info(f"{clean_table} check - {desc}: {count}")
        print(f"{clean_table} check - {desc}: {count}")

def main():  # Function: main entry point to connect to DuckDB and clean both yellow and green 2024 tables
    con = duckdb.connect(DB_PATH, read_only=False)

    # Clean both yellow and green trips
    clean_table(con, "yellow_trips_2024", "yellow_clean")
    clean_table(con, "green_trips_2024", "green_clean")

    con.close()

if __name__ == "__main__":  # Script entry point
    main()
