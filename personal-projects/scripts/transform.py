import pandas as pd

def transform_data(all_countries):
    df = pd.DataFrame(all_countries)


    # Data Cleaning and Transformation
    df["region_name"] = df["region"].apply(lambda x: x["value"])
    df["Income_level"] = df["incomeLevel"].apply(lambda x: x["value"])
    df = df[df["capitalCity"] != ""]


    clean_df = df[["id", "name", "region_name", "Income_level"]]
    return clean_df