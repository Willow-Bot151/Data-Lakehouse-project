import pandas as pd
import json
import boto3

def grab_currency_conversion_info():
    with open("src/processing/reference_data/currency_information_data.json", "r") as file:
        currency_data = json.load(file)
        return currency_data

def create_dim_currency(df):
    
    currency_dict = grab_currency_conversion_info()
    
    currency_info_df = pd.DataFrame(list(currency_dict['currencies'].items()), columns=['currency_code', 'currency_name'])

    clean_df = df.drop(columns=["created_at", "last_updated"])

    merged_df = clean_df.merge(currency_info_df, on='currency_code', how='left')
    
    return merged_df
    