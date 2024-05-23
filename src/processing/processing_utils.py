import pandas as pd
import json
import datetime
import awswrangler as wr
import botocore

def df_normalisation(df,table_name):
    if table_name in df.columns:
        df_norm = pd.json_normalize(df[table_name])
    return df_norm

# purpose to read timestamp from ingestion bucket
def read_timestamp_from_s3(bucket, key,s3_client):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    return data

def extract_timestamp_from_key(key):
    timestamp_str = key.split('--')[1]
    return datetime.datetime.strptime(timestamp_str, '%m:%d:%Y-%H:%M:%S')


def filter_files_by_timestamp(bucket_name,prefix,objects, start_time, end_time):
    filtered_files = []
    for obj in objects:
        key = obj.split('/')[-1]
        timestamp = extract_timestamp_from_key(key)
        if timestamp and start_time <= timestamp <= end_time:
            filtered_files.append(f's3://{bucket_name}/{prefix}{key}')
    return filtered_files

def df_to_parquet(df):
    return df.to_parquet()


def list_objects_in_bucket(bucket_name,prefix):
    objects = wr.s3.list_objects(f's3://{bucket_name}/{prefix}')
    print(objects) 
    return objects


def init_s3_client():
    session = botocore.session.get_session()
    s3_client = session.create_client("s3")
    return s3_client

def write_parquet_S3(s3_client,parquet,table):

    s3_client.put_object(
        Body=parquet,
        Bucket="nc-team-reveries-processing",
        Key=f"{table}/-data",
    )

def write_timestamp_to_s3(s3_client, timestamp):
    s3_client.put_object(
        Body= timestamp,
        Bucket="nc-team-reveries-processing",
        Key='timestamp'
    )