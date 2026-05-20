import requests
import pandas as pd

base_url = "https://api.worldbank.org/v2/country/all"

all_countries = []

page = 1

while True:

    params = {
        "format": "json",
        "page": page
    }

    try:

        response = requests.get(
            base_url,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        data = response.json()

        metadata = data[0]
        countries = data[1]

        all_countries.extend(countries)

        print(f"Collected page {page}")

        if page >= metadata["pages"]:
            break

        page += 1

    except requests.exceptions.RequestException as e:

        print(f"API request failed: {e}")
        break

df = pd.DataFrame(all_countries)
df.to_csv("countries.csv", index=False)
df.to_json("countries.json")
print(df.shape)