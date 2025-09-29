import os
import requests

# Base URL where monthly NYC Taxi trip parquet files are hosted
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# Local directory where data will be saved
DATA_DIR = "data"

# Make sure the local data directory exists (create it if missing)
os.makedirs(DATA_DIR, exist_ok=True)

def download_file(url, dest):
    """
    Download a file from a URL to a local destination.
    Skips download if file already exists.
    """
    if os.path.exists(dest):
        print(f"Already downloaded: {dest}")
        return
    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        # Save file in chunks to avoid memory issues with large files
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved to {dest}")
    else:
        print(f"Failed to download {url} (status {response.status_code})")

# Loop through all 12 months of 2024 and download yellow and green taxi data
for month in range(1, 13):
    month_str = f"{month:02d}"  # Format month as two digits (e.g., 01, 02, … 12)
    
    # Construct URLs for yellow and green taxi parquet files
    yellow_url = f"{BASE_URL}/yellow_tripdata_2024-{month_str}.parquet"
    green_url = f"{BASE_URL}/green_tripdata_2024-{month_str}.parquet"

    # Define destination paths inside the local data folder
    yellow_dest = os.path.join(DATA_DIR, f"yellow_tripdata_2024-{month_str}.parquet")
    green_dest = os.path.join(DATA_DIR, f"green_tripdata_2024-{month_str}.parquet")

    # Download both yellow and green files for this month
    download_file(yellow_url, yellow_dest)
    download_file(green_url, green_dest)

