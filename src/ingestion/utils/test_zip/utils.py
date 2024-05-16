import json
import datetime
import boto3
import json
from pg8000.native import identifier, literal
import os
import pprint

def get_current_timestamp():
    s3_client = boto3.client("s3")
    
    response = s3_client.get_object(
            Bucket="ldcm-python-test",
            Key="timestamp"
    )
    body = response['Body'].read()
    dt_str = json.loads(body.decode('utf-8'))
    dt = datetime.datetime.fromisoformat(dt_str)
    return dt


def query_updated_table_information(conn, table, dt):
    query = f"""SELECT * 
                FROM {identifier(table)} 
                WHERE last_updated > {literal(dt)}
                ORDER BY last_updated ASC
                LIMIT 10;"""
    result = conn.run(query)
    columns = [col["name"]for col in conn.columns]
    output_table = put_into_individual_table(table, result, columns)
    return output_table        
            
                
def put_into_individual_table(table, result, columns):
                individual_table = {f"{table}": [dict(zip(columns, line)) for line in result]}
                
                return individual_table

def get_datestamp_from_table(individual_table):
                timestamp = individual_table['sales_order'][-1]['last_updated']
                
                return timestamp
                
def get_datetime_now():
        now = datetime.datetime.now() 
        date_time = now.strftime("%m:%d:%Y-%H:%M:%S")
        return date_time

def put_object_in_bucket(table, put_table):
                s3_client = boto3.client("s3")

                date_time = get_datetime_now()
                
                s3_client.put_object(
                    Body=json.dumps(str(put_table)),
                    Bucket="ldcm-python-test",
                    Key=f"{table}/--{date_time}--{table}-data"
                )
            
def put_timestamp_in_s3(timestamp):
    s3_client = boto3.client("s3")
    dt = s3_client.put_object(
    Body=json.dumps(str(timestamp)),
    Bucket="ldcm-python-test",
    Key=f"timestamp"
    )   
    return dt

def initialise_bucket_with_timestamp():
    s3_client = boto3.client("s3")
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="ldcm-python-test",
        Key=f"timestamp"
    )
