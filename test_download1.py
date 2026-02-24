import requests
import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor

API_KEY = "YUeQMd30agr1TEdtYzGl36AlBmAanEv8wqN7YF2Z"
BASE_URL = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

length = 5000


def download_year(year):

    offset = 0
    file_name = f"eia_fueltype_{year}.csv"  

    print(f"Starting {year}")

    while True:

        params = {
            "api_key": API_KEY,
            "frequency": "hourly",
            "data[0]": "value",
            "start": f"{year}-01-01T00",
            "end": f"{year}-12-31T00",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": length
        }

        for attempt in range(5):
            try:
                r = requests.get(BASE_URL, params=params, timeout=60)

                if r.status_code == 200:
                    data = r.json()
                    break
            except:
                time.sleep(5)
        else:
            print(f"{year}: skipped offset {offset}")
            offset += length
            continue

        if "response" not in data or len(data["response"]["data"]) == 0:
            print(f"{year} finished")
            break

        df = pd.DataFrame(data["response"]["data"])

        df.to_csv(
            file_name,
            mode="a",
            header=not os.path.exists(file_name),
            index=False
        )

        print(f"{year}: saved {offset} → {offset + len(df)}")

        offset += length
        time.sleep(0.5)


years = [2020, 2021, 2022, 2023, 2024, 2025]

with ThreadPoolExecutor(max_workers=6) as executor:
    executor.map(download_year, years)

print("All downloads completed.")