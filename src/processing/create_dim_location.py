import pandas as pd
import json
from src.processing.processing_utils import df_to_parquet,write_parquet_file_to_s3
'''
final columns needed:
        location_id             primary key to be added
        address_line_1          from address table
        address_line_2          from address table
        district                from address table
        city                    from address table
        postal_code             from adress table
        country                 from address table
        phone                   from address table
    
    - read data from address table
    - massage columns to drop some columns from DF
    - return DF

'''

def create_dim_location(address_tbl_df):

    modified_dim_location_df = address_tbl_df.rename(columns={'address_id': 'location_id'})

    # dropping the columns which are not required from dataframe
    modified_dim_location_df.pop('created_at')
    modified_dim_location_df.pop('last_updated')

    return modified_dim_location_df
