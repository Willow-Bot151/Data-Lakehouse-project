from pg8000.native import Connection
import os
from dotenv import load_dotenv
from pg8000.native import literal, identifier
import datetime
from src.ingestion.utils.test_zip.utils import convert_datetimes_and_decimals, put_object_in_bucket
from moto import mock_aws
import boto3
import pytest
import json

load_dotenv(".env")

user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
database = os.getenv("PG_DATABASE")
host = os.getenv("PG_HOST")
port = int(os.getenv("PG_PORT"))

def connect_to_db():
    return Connection(
        user=user,
        password=password,
        database=database,
        host=host,
        port=port
    )

def close_connection(conn):
    conn.close()

@pytest.fixture(scope="class")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def mock_s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")

@pytest.fixture()
def get_sample_data_from_db():
    conn=connect_to_db()  
    query = f"""SELECT *
                FROM {identifier('sales_order')}
                WHERE last_updated > {literal(datetime.datetime(2022,1,1,13,20,22))}
                ORDER BY last_updated ASC
                LIMIT 2;"""
    result = conn.run(query) 
    columns = [col["name"] for col in conn.columns]
    individual_table = {"sales_order": [dict(zip(columns, line)) for line in result]}
    return individual_table

def test_data_is_json(get_sample_data_from_db,mock_s3_client):
    mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={
                'LocationConstraint': "eu-west-2"
            }
        )

    table=get_sample_data_from_db
    json_table=convert_datetimes_and_decimals(table)
    put_object_in_bucket('sales_order',json_table,mock_s3_client,'nc-team-reveries-ingestion')
    listed_objects = mock_s3_client.list_objects(
            Bucket="nc-team-reveries-ingestion"
        )
    returned_object = mock_s3_client.get_object(
            Bucket="nc-team-reveries-ingestion",
            Key=listed_objects['Contents'][0]['Key']
        )
    test_var=json.loads(returned_object['Body'].read().decode('utf-8'))

    assert json_table == test_var

