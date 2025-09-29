# analysis.py
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import os
import logging

# --- Logging configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="analysis.log"
)
logger = logging.getLogger(__name__)

DB_PATH = "emissions.duckdb"
OUT_DIR = "outputs"

DAY_NAMES = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
MONTH_NAMES = {
    1: "Jan",  2: "Feb",  3: "Mar",  4: "Apr",
    5: "May",  6: "Jun",  7: "Jul",  8: "Aug",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}

def table_exists(conn, name: str) -> bool:
    """Check if a given table exists in the DuckDB database."""
    exists = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [name]
    ).fetchone()[0] > 0
    logger.info(f"Checked existence of table {name}: {exists}")
    return exists

def largest_trip(conn, table: str):
    """Return the largest carbon-producing trip (CO₂ and distance) from a given table."""
    logger.info(f"Querying largest CO₂ trip from {table}")
    return conn.execute(f"""
        SELECT trip_co2_kgs, trip_distance
        FROM {table}
        ORDER BY trip_co2_kgs DESC
        LIMIT 1
    """).fetchone()

def heaviest_and_lightest(conn, table: str, col: str):
    """Return the periods (max and min) with the heaviest and lightest average CO₂ emissions."""
    logger.info(f"Calculating heaviest and lightest by {col} from {table}")
    df = conn.execute(f"""
        SELECT {col} AS period, AVG(trip_co2_kgs) AS avg_co2, COUNT(*) AS n
        FROM {table}
        GROUP BY {col}
        HAVING COUNT(*) > 0
        ORDER BY avg_co2 DESC
    """).fetchdf()
    if df.empty:
        logger.warning(f"No data returned for {col} in {table}")
        return None, None
    return df.iloc[0].to_dict(), df.iloc[-1].to_dict()

def monthly_totals(conn, table: str) -> pd.DataFrame:
    """Fetch total CO₂ emissions grouped by month from a given table."""
    logger.info(f"Fetching monthly totals for {table}")
    return conn.execute(f"""
        SELECT month_of_year AS month, SUM(trip_co2_kgs) AS total_co2_kg
        FROM {table}
        GROUP BY month_of_year
        ORDER BY month_of_year
    """).fetchdf()

def print_heavy_light(label: str, max_row: dict, min_row: dict, transform=None):
    """Print the heaviest and lightest periods with labels and log the results."""
    if not max_row or not min_row:
        logger.warning(f"{label}: No data available to report.")
        print(f"{label}: (no data)")
        return
    max_p = int(max_row["period"])
    min_p = int(min_row["period"])
    max_label = transform(max_p) if transform else max_p
    min_label = transform(min_p) if transform else min_p
    logger.info(f"{label} — MOST={max_label}, LEAST={min_label}")
    print(f"{label} — MOST carbon-heavy:   {max_label} (avg kg = {max_row['avg_co2']:.4f}, n = {int(max_row['n'])})")
    print(f"{label} — LEAST carbon-heavy:  {min_label} (avg kg = {min_row['avg_co2']:.4f}, n = {int(min_row['n'])})")

def main():
    """Main function: runs all analysis steps for both Yellow and Green taxi data and generates plots."""
    os.makedirs(OUT_DIR, exist_ok=True)
    logger.info("Starting analysis.py")
    
    conn = duckdb.connect(DB_PATH, read_only=True)
    logger.info("Connected to DuckDB")

    for cab, table in [("YELLOW", "yellow_transformed"), ("GREEN", "green_transformed")]:
        if not table_exists(conn, table):
            msg = f"[{cab}] Table '{table}' not found. Did you run transform.py?"
            print(msg)
            logger.error(msg)
            continue

        print(f"\n========== {cab} ==========")
        logger.info(f"Running analysis for {cab} using {table}")

        # Largest carbon-producing trip
        row = largest_trip(conn, table)
        if row:
            co2_kg, dist = row
            print(f"[{cab}] Largest CO₂ trip: {co2_kg:.4f} kg (distance = {dist:.2f} miles)")
            logger.info(f"{cab} largest CO₂ trip: {co2_kg:.4f} kg (distance = {dist:.2f} miles)")

        # Most/least carbon-heavy by hour, day, week, month
        max_row, min_row = heaviest_and_lightest(conn, table, "hour_of_day")
        print_heavy_light(f"[{cab}] Hour of Day (1–24)", max_row, min_row, transform=lambda h: (h % 24) + 1)

        max_row, min_row = heaviest_and_lightest(conn, table, "day_of_week")
        print_heavy_light(f"[{cab}] Day of Week (Sun–Sat)", max_row, min_row, transform=lambda d: DAY_NAMES.get(d, d))

        max_row, min_row = heaviest_and_lightest(conn, table, "week_of_year")
        print_heavy_light(f"[{cab}] Week of Year (0–52)", max_row, min_row)

        max_row, min_row = heaviest_and_lightest(conn, table, "month_of_year")
        print_heavy_light(f"[{cab}] Month of Year (Jan–Dec)", max_row, min_row, transform=lambda m: MONTH_NAMES.get(m, m))

        # Plotting CO₂ totals
        df = monthly_totals(conn, table).set_index("month")
        plt.figure(figsize=(10, 5))
        plt.plot(df.index, df["total_co2_kg"], marker="o", label=f"{cab} totals")
        plt.xticks(range(1, 13), [MONTH_NAMES[m] for m in range(1, 13)])
        plt.xlabel("Month")
        plt.ylabel("Total CO₂ (kg)")
        plt.title(f"Monthly Taxi Trip CO₂ Totals (2024) — {cab}")
        plt.legend()
        path = os.path.join(OUT_DIR, f"{cab.lower()}_co2_by_month_totals.png")
        plt.savefig(path)
        plt.close()
        logger.info(f"Saved plot for {cab} to {path}")
        print(f"Saved plot to {path}")

    conn.close()
    logger.info("Closed DuckDB connection")
    logger.info("Finished analysis.py")

if __name__ == "__main__":
    main()



