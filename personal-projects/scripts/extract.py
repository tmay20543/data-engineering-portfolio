import requests


all_countries = []

page = 1

def extract_countries(base_url):
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
    return all_countries