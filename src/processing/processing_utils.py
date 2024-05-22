import pandas as pd

def df_normalisation(df,table_name):
    if table_name in df.columns:
        df_norm = pd.json_normalize(df[table_name])
    return df_norm