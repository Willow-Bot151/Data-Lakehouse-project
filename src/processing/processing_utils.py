import pandas as pd
import json
import datetime
import awswrangler as wr
import botocore
from botocore.exceptions import ClientError

def df_normalisation(df,table_name):
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
            filtered_files.append(f's3://{bucket_name}/{prefix}/{key}')
    return filtered_files

def df_to_parquet(df):
    return df.to_parquet()


def list_objects_in_bucket(bucket_name,prefix):
    objects = wr.s3.list_objects(f's3://{bucket_name}/{prefix}/')
    return objects

def write_parquet_file_to_s3(file, s3_client, bucket_name, table_name, date_start, date_end):
    key = f"{table_name}/{date_start}_{date_end}_entries"
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=file 
    )

def init_s3_client():
    try: 
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        return s3_client
    except ClientError as e:
        raise e("-ERROR- Failure to connect to s3 client, please check credentials")
                


def write_timestamp_to_s3(s3_client, bucket_name, timestamp):
    s3_client.put_object(
        Body= timestamp,
        Bucket=bucket_name,
        Key='timestamp'
    )

def initialise_processing_bucket_with_timestamp(s3_client):
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="nc-team-reveries-processing",
        Key=f"timestamp",
    )

