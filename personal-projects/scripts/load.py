import pandas as pd

def load_data(transformed_data):
    df.to_csv("countries.csv", index=False)
    df.to_json("countries.json")
    clean_df.to_csv("countries_clean.csv", index=False)

    print(df.shape)