import boto3
import json
import datetime
import pandas as pd
import awswrangler as wr

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


def s3_reader_many_files():
    bucket_name = 'nc-team-reveries-ingestion'
    file_key = 'transaction_test'
    df = wr.s3.read_json(path=f's3://{bucket_name}/{file_key}/')
    if 'transaction' in df.columns:
        df = pd.json_normalize(df['transaction'])
    print(df)
    return df


#last_modified_begin – Filter the s3 files by the Last modified date of the object.
    # The filter is applied only after list all s3 files.
#last_modified_end (datetime, optional) – Filter the s3 files by the Last
   # modified date of the object. The filter is applied only after list all s3 files.
# df=s3_reader_many_files()
# print(df)
