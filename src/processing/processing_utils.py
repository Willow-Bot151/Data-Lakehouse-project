import pandas as pd
import boto3
import json
import datetime
import awswrangler as wr

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

"""
Organise data with a key of -dim/fact name- and then name the file -a parquet file with a datestamp between two points of time-

{table_name}/{date_start}-{date_end}/entries

Function requirements (parquet_data, )
Clean data for null/bad values :(
For loop to iterate over rows?
"""
def write_parquet_file_to_s3(file, s3_client, bucket_name, table_name, date_start, date_end):
    key = f"{table_name}/{date_start}_{date_end}_entries"
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=file 
    )