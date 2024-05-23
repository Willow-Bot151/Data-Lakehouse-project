from src.processing.processing_utils import df_normalisation, read_timestamp_from_s3,\
    extract_timestamp_from_key,filter_files_by_timestamp, df_to_parquet
import pytest
import json
import boto3
import os
import datetime
import pandas as pd
from moto import mock_aws
from unittest.mock import patch

@pytest.fixture
def test_data_to_df():
    with open('testing/processing/test_data.json') as f:
        test_data= json.load(f)
    df = pd.DataFrame(test_data)
    return df

@pytest.fixture(scope='function')
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2')

@pytest.fixture(scope='function')
def aws_creds():
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'

with open('testing/processing/test_date.txt') as f:
        timestamp_data = f.read()

@pytest.fixture
def dummy_ingestion_bucket(s3_client):
    s3_client.create_bucket(
        Bucket='dummy_ingestion_bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
    
    with open('testing/processing/test_date.txt') as f:
        timestamp_data = f.read()

        #print('timestamp:',timestamp_data)
    s3_client.put_object(
            Body=timestamp_data, Bucket='dummy_ingestion_bucket',
            Key='test_date.txt')

class TestDfNorm:
    def test_df_normalisation_does_what_its_says(self,test_data_to_df):
        test_df=test_data_to_df
        remodelled_df=df_normalisation(test_df,'transaction')
        assert isinstance(remodelled_df,pd.DataFrame)
        assert remodelled_df.shape == (10,6)
        assert list(remodelled_df.columns) == ['transaction_id', 'transaction_type', \
                                         'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']
        assert remodelled_df['created_at'][0] == '2022-11-03T14:20:52.186000'

class TestTimeStampReader:
    def test_read_timestamp_from_s3(self,dummy_ingestion_bucket,s3_client):
        result = s3_client.list_objects_v2(Bucket='dummy_ingestion_bucket')
        assert len(result["Contents"]) == 1
        assert result["Contents"][0]["Key"] == "test_date.txt"

    def test_read_timestamp_from_s3_returns_data(self,dummy_ingestion_bucket,s3_client):
        result = read_timestamp_from_s3('dummy_ingestion_bucket','test_date.txt',s3_client)
        assert isinstance(result, str)
        assert result == "2022-11-22T15:02:10.226000"
 
class TestExtractAndFilterTimestampFromKey:
    def test_extract_timestamp_from_key(self):
        assert extract_timestamp_from_key("--05:22:2024-08:28:06--purchase_order-data") == datetime.datetime(2024, 5, 22, 8, 28, 6)
        assert extract_timestamp_from_key("--05:22:2024-08:28:06--") == datetime.datetime(2024, 5, 22, 8, 28, 6)

    @patch('awswrangler.s3.list_objects')
    def test_filter_files_by_timestamp(self,mock_list_objects):
        mock_list_objects.return_value = [
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-08:28:06--purchase_order-data',
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-08:28:23--purchase_order-data',
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-09:42:27--purchase_order-data',
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-08:00:00--purchase_order-data']
        start_time = datetime.datetime(2024, 5, 22, 8, 28, 0)
        end_time = datetime.datetime(2024, 5, 22, 9, 0, 0)

        expected_files = [
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-08:28:06--purchase_order-data',
            's3://nc-team-reveries-ingestion/transaction/--05:22:2024-08:28:23--purchase_order-data']
        bucket_name = 'nc-team-reveries-ingestion'
        prefix = 'transaction/'
        result = filter_files_by_timestamp(bucket_name,prefix,mock_list_objects.return_value, start_time, end_time)
        assert result == expected_files

class TestDfToParquet:
    def test_df_to_parquet(self, test_data_to_df):
        test_df = test_data_to_df
        expected = test_df.to_parquet()
        result=df_to_parquet(test_df)
        assert result == expected
        assert isinstance(result, bytes)

