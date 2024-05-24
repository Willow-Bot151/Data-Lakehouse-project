import pandas as pd

def create_dim_counterparty(address_df, counterparty_df):
    renamed_counterparty_df = counterparty_df.rename(columns={
        "legal_address_id":"address_id"
        })
    renamed_address_df = address_df.rename(columns={
        "address_line_1":"counterparty_legal_address_line_1",
        "address_line_2":"counterparty_legal_address_line2",
        "district":"counterparty_legal_district",
        "city":"counterparty_legal_city",
        "postal_code":"counterparty_legal_postal_code",
        "country":"counterparty_legal_country",
        "phone":"counterparty_legal_phone"
        })
    big_df = pd.merge(renamed_address_df, renamed_counterparty_df, on="address_id")
    print(big_df.columns)
    required_columns = [
        'counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_address_line2',
        'counterparty_legal_district',
        'counterparty_legal_city',
        'counterparty_legal_postcode',
        'counterparty_legal_country',
        'counterparty_legal_phone_number'
        ]
    for i in required_columns:
        if i in big_df.columns:
            pass
        else:
            raise Exception("not there")
    dim_df = big_df.filter(required_columns)
    print(dim_df)
    return dim_df

def clean_my_dataframe(not_null_list,int_list,string_list):
    non_nulls = ['counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_city',
        'counterparty_legal_postcode',
        'counterparty_legal_country',
        'counterparty_legal_phone_number']
    
    strings = ['counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_address_line2',
        'counterparty_legal_district',
        'counterparty_legal_city',
        'counterparty_legal_postcode',
        'counterparty_legal_country',
        'counterparty_legal_phone_number']
    ints = ['counterparty_id']
    # return dim_df

def search_files_for_data(
        table_name, 
        key_to_find_list, 
        s3_client,
        bucket_name ):
    """
    If new counterparties are ingested, they may not include new addresses in the same ingestion.
    Therefore a function to search previous addresses may be neccessary as an extension.
    """
    pass