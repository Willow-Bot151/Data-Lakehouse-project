import boto3
import json
import datetime
import pandas as pd
import awswrangler as wr
from src.processing.processing_utils import df_normalisation

def s3_file_reader_local(input):
    data=input['transaction']
    #print(data)
    df = pd.DataFrame(data)
    print(df['transaction_id'][0])
    return df

def s3_file_reader_remote(bucket, key, s3_client):
    #s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    df = pd.DataFrame(json.loads(json_data)['transaction'])
    return df 


def s3_reader_many_files(table):
    bucket_name = 'nc-team-reveries-ingestion'
    file_key = table
    df = wr.s3.read_json(path=f's3://{bucket_name}/{file_key}/')
    if table in df.columns:
        df_norm = df_normalisation(df,table)
        return df_norm
    else:
        return df


def s3_reader_filtered(table,filtered_files):
    df = wr.s3.read_json(path=filtered_files)
    if table in df.columns:
        df_norm = df_normalisation(df,table)
        print(df_norm.shape)
        print(df_norm)
        return df_norm
    else:
        return df

