import extract
import transform
import load
import os

base_url = os.getenv("BASE_URL", "https://api.worldbank.org/v2/country/all")

def main():

    # Step 1: Extract data
    data = extract.extract_countries(base_url)
    
    # Step 2: Transform data
    transformed_data = transform.transform_data(data)
    
    # Step 3: Load data
    load.load_data(transformed_data)

if __name__ == "__main__":
    main()   