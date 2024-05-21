from src.processing.s3_file_reader import s3_file_reader_local, s3_file_reader_remote, s3_reader_many_files
import pytest
import json
import boto3
import os
import pandas as pd
from moto import mock_aws
from unittest.mock import patch

with open('testing/processing/test_data.json') as f:
    test_transaction_data = json.load(f)

@pytest.fixture(scope='function')
def aws_creds():
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'

@pytest.fixture(scope='function')
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2')

@pytest.fixture
def dummy_ingestion_bucket(s3_client):
    s3_client.create_bucket(
        Bucket='dummy_ingestion_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
    
    with open('testing/processing/test_data.json') as f:
        transaction_data = json.load(f)

    s3_client.put_object(
            Body=json.dumps(transaction_data), Bucket='dummy_ingestion_bucket',
            Key='test_data.json')


class TestS3FileReaderLocal:
    def test_s3_file_reader_local_returns_a_dataframe(self):
        input=test_transaction_data
        result=s3_file_reader_local(input)
        assert isinstance(result, pd.DataFrame)

    def test_s3_file_reader_local_returns_correct_data_for_input_file(self):
        input=test_transaction_data
        result=s3_file_reader_local(input)
        assert result['transaction_id'][0] == 8
        assert result['transaction_id'][9] == 7      
        assert result['transaction_type'][9] == 'SALE' 
        assert result['transaction_type'][2] == 'PURCHASE'      
        assert result['created_at'][4] == "2022-11-03T14:20:52.186000"
        assert result['created_at'][8] == "2022-11-03T14:20:52.188000"



class TestS3FileReaderRemote:
    def test_dummy_ingestion_bucket_created(self,s3_client,dummy_ingestion_bucket):
        result = s3_client.list_buckets()
        assert len(result["Buckets"]) == 1
        assert result["Buckets"][0]["Name"] == "dummy_ingestion_bucket"

    def test_dummy_ingestion_bucket_contains_test_json_files(self, s3_client, dummy_ingestion_bucket):
        result = s3_client.list_objects_v2(Bucket='dummy_ingestion_bucket')
        print(len(result["Contents"]))
        assert len(result["Contents"]) == 1
        assert result["Contents"][0]["Key"] == "test_data.json"


    def test_s3_file_reader_remote_extracts_data_from_ingestion_s3(self, dummy_ingestion_bucket,s3_client):
        input=test_transaction_data
        result = s3_file_reader_remote('dummy_ingestion_bucket','test_data.json',s3_client)
        print(result)
        assert isinstance(result, pd.DataFrame)
        assert result['transaction_id'][0] == 8
        assert result['transaction_id'][9] == 7      
        assert result['transaction_type'][9] == 'SALE' 
        assert result['transaction_type'][2] == 'PURCHASE'      
        assert result['created_at'][4] == "2022-11-03T14:20:52.186000"
        assert result['created_at'][8] == "2022-11-03T14:20:52.188000"



class TestS3ReaderManyFiles:
    def test_s3_reader_many_files_returns_correct_data(self,mocker):  
        df_result = pd.read_pickle("testing/processing/transaction_df.pkl")
        mocker.patch('awswrangler.s3.read_json', return_value=df_result)
        result_df = s3_reader_many_files()
        assert isinstance(result_df,pd.DataFrame)
        assert result_df.shape == (10,6)
        assert list(result_df.columns) == ['transaction_id', 'transaction_type', \
                                         'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']
        assert result_df['created_at'][0] == '2022-11-03T14:20:52.186000'

