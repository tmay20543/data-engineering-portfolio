import requests
import pandas as pd

url = "https://api.worldbank.org/v2/country/all?format=json"

response = requests.get(url)

print("Status:", response.status_code)

data = response.json()

countries = data[1]

df = pd.DataFrame(countries)

print(df.head())