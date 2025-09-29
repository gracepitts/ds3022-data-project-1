import os
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = "data"

# Make sure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def download_file(url, dest):
    if os.path.exists(dest):
        print(f"Already downloaded: {dest}")
        return
    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Saved to {dest}")
    else:
        print(f"Failed to download {url} (status {response.status_code})")

# Loop through years and months
for year in range(2015, 2025):   # 2015–2024 inclusive
    for month in range(1, 13):
        month_str = f"{month:02d}"
        
        yellow_url = f"{BASE_URL}/yellow_tripdata_{year}-{month_str}.parquet"
        green_url = f"{BASE_URL}/green_tripdata_{year}-{month_str}.parquet"

        yellow_dest = os.path.join(DATA_DIR, f"yellow_tripdata_{year}-{month_str}.parquet")
        green_dest = os.path.join(DATA_DIR, f"green_tripdata_{year}-{month_str}.parquet")

        download_file(yellow_url, yellow_dest)
        download_file(green_url, green_dest)
