import os
import requests
import duckdb
import logging

# --- Setup ---
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = "data"
DB_PATH = "emissions.duckdb"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="load.log"
)
logger = logging.getLogger(__name__)

# --- Helpers ---
def download_file(url, dest):
    """Download a file from URL if not already present locally."""
    if os.path.exists(dest):
        logger.info(f"Already downloaded: {dest}")
        return
    logger.info(f"Downloading {url} ...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Saved to {dest}")
    else:
        logger.error(f"Failed to download {url} (status {response.status_code})")
        raise RuntimeError(f"Download failed: {url}")

def load_parquet_files(con, table_name, files):
    """Create a DuckDB table from multiple parquet files."""
    # Drop table if it exists
    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    # Load all files in one UNION ALL query
    query = " UNION ALL ".join([f"SELECT * FROM read_parquet('{f}')" for f in files])
    con.execute(f"CREATE TABLE {table_name} AS {query}")
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    logger.info(f"Loaded {count} rows into {table_name}")
    print(f"{table_name}: {count} rows")

# --- Main ---
def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DB_PATH, read_only=False)

    # Loop through 12 months of 2024
    yellow_files = []
    green_files = []
    for month in range(1, 13):
        month_str = f"{month:02d}"

        # Yellow taxi
        yellow_url = f"{BASE_URL}/yellow_tripdata_2024-{month_str}.parquet"
        yellow_dest = os.path.join(DATA_DIR, f"yellow_tripdata_2024-{month_str}.parquet")
        download_file(yellow_url, yellow_dest)
        yellow_files.append(yellow_dest)

        # Green taxi
        green_url = f"{BASE_URL}/green_tripdata_2024-{month_str}.parquet"
        green_dest = os.path.join(DATA_DIR, f"green_tripdata_2024-{month_str}.parquet")
        download_file(green_url, green_dest)
        green_files.append(green_dest)

    # Load Yellow and Green into DuckDB
    load_parquet_files(con, "yellow_trips", yellow_files)
    load_parquet_files(con, "green_trips", green_files)

    # Load emissions lookup table
    con.execute("DROP TABLE IF EXISTS vehicle_emissions")
    con.execute("CREATE TABLE vehicle_emissions AS SELECT * FROM read_csv_auto('data/vehicle_emissions.csv', HEADER=TRUE)")
    emissions_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
    logger.info(f"Loaded {emissions_count} rows into vehicle_emissions")
    print(f"vehicle_emissions: {emissions_count} rows")

    con.close()

if __name__ == "__main__":
    main()

