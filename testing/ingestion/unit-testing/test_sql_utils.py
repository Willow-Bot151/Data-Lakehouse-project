import pytest
from src.ingestion.utils.test_zip.utils import get_current_timestamp, get_datestamp_from_table, get_datetime_now, put_into_individual_table, put_object_in_bucket, query_updated_table_information
import datetime
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from moto import mock_aws
import boto3
import os
import json

@pytest.fixture
def get_table_names():
    tables = [
        'counterparty',
        'currency',
        'department',
        'design,staff',
        'sales_order',
        'address',
        'payment',
        'purchase_order',
        'payment_type',
        'transaction'
        ]
    return tables

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
        

@pytest.fixture
def mock_boto3_client():
    with patch("boto3.client") as mock_client:
        yield mock_client

class TestGetCurrentTimestamp:
    def test_get_currrent_timestamp_returns_a_datetime_object(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={
                'LocationConstraint': "eu-west-2"
            }
        )
        mock_s3_client.put_object(
            Bucket="nc-team-reveries-ingestion",
            Key="timestamp",
            Body= json.dumps("2024-05-16T12:00:00")
        )
        
        result = get_current_timestamp(mock_s3_client)
        assert result.isoformat() == "2024-05-16T12:00:00"

    def test_raises_error_with_no_current_timestamp(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={
                'LocationConstraint': "eu-west-2"
            }
        )

        with pytest.raises(Exception):
            get_current_timestamp(mock_s3_client)

    def test_when_timestamp_format_is_incorrect(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={
                'LocationConstraint': "eu-west-2"
            }
        )
        
        mock_s3_client.put_object(
            Bucket="nc-team-reveries-ingestion",
            Key="timestamp",
            Body= "incorrect test string"
        )
        with pytest.raises(Exception):
            get_current_timestamp(mock_s3_client)
    

@pytest.fixture
def mock_conn():
    return MagicMock()

@pytest.fixture
def mock_conn1():
    class MockConnection:
        def __init__(self):
            self.columns = [{"name": "id"}, {"name": "name"}, {"name": "last_updated"}]
        def run(self, query):
            raise Exception("Database error")
    return MockConnection()

class TestQueryUpdatedTableInformation:
    @pytest.mark.skip("This is broken")
    def test_query_updated_table_information_empty_result(self,mock_conn):
        mock_conn.run.return_value = []
        dt = "2024-05-16T11:00:00"
        table = "example_table"
        output_table = query_updated_table_information(mock_conn, table, dt)
        assert len(output_table[table]) == 0
    @pytest.mark.skip("This is broken")
    def test_query_updated_table_information_returns_len_data_in_specific_range(self,mock_conn):
        mock_conn.run.return_value = [
            {"id": 1, "name": "Cameron", "last_updated": "2024-05-16T12:00:00"},
            {"id": 2, "name": "Luke", "last_updated": "2024-05-16T13:00:00"}]
        mock_conn.columns = [{"name": "id"}, {"name": "name"}, {"name": "last_updated"}]
        dt = "2024-05-16T11:00:00"
        table = "test_table"
        return_table = query_updated_table_information(mock_conn, table, dt)
        print(return_table)
        assert len(return_table[table]) == 2
        # assert len(return_table[table]) < 10

    @pytest.mark.skip
    def test_query_updated_table_information_malformed_data(self,mock_conn):
        """
        Test for data rejection where last updated is missing
        """
        expected_column_names = ["id", "name", "last_updated"]
        mock_data = [
            {"id": 1, "name": "Cameron"},  # Missing 'last_updated' column
            {"id": 2, "name": "Luke", "last_updated": "2024-05-16T13:00:00"}]
        mock_conn.run.return_value = mock_data
        mock_conn.columns = [{"name": "id"}, {"name": "name"}, {"name": "last_updated"}]
        actual_column_names = [col["name"] for col in mock_conn.columns]
        dt = "2024-05-16T11:00:00"
        table = "test_table"
        output_table = query_updated_table_information(mock_conn, table, dt)
        assert len(output_table[table]) == len(mock_data)
        assert actual_column_names == expected_column_names
        assert len(output_table[table][0]) == 2

    def test_query_updated_table_information_handles_database_error(mock_conn):
        output_table = query_updated_table_information(mock_conn1, "test_table", "2024-05-16T11:00:00")
        assert output_table is None

def test_get_datestamp_from_table_returns_timestamp():
    #timestamp should = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    test_table_name = 'sales_order'
    fake_data = {
    test_table_name: [
        {'last_updated': '2022-03-12 19:24:01:377'},
        {'last_updated': '2024-05-14 16:45:09.72'},
        {'last_updated': '2024-05-14 16:54:10.308'}
    ]
}
    result = get_datestamp_from_table(fake_data, test_table_name)
    expected = '2024-05-14 16:54:10.308'
    assert result == expected
    assert type(result) == str
    assert len(result) == len(expected)

def test_get_datestamp_from_table_error_handling():
    pass

def test_get_datetime_now_converts_to_strftime():
    test_func = get_datetime_now()
    now = datetime.datetime.now()
    expected = now.strftime("%m:%d:%Y-%H:%M:%S")
    assert test_func == expected
    assert type(test_func) == str

class TestPutIntoIndividualTable:
    def test_put_into_individual_table_returns_dict(self):
        table = "test_sales_order"
        columns = ["sales_order_id", "created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]
        result = [
            [8256, "2024-05-14 16:54:10.308", "2024-05-14 16:54:10.308", 311, 1, 20, 99847, 3.80, 2, "2024-05-18", "2024-05-18", 6]
        ]
        individual_table = put_into_individual_table(table, result, columns)
        assert isinstance(individual_table, dict)

    def test_into_individual_table_matches_correct_key_pairs(self):
        table = "test_sales_order"
        columns = ["sales_order_id", "created_at", "last_updated", "design_id", "staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "agreed_delivery_date", "agreed_payment_date", "agreed_delivery_location_id"]
        result = [
            [8256, "2024-05-14 16:54:10.308", "2024-05-14 16:54:10.308", 311, 1, 20, 99847, 3.80, 2, "2024-05-18", "2024-05-18", 6]
        ]
        individual_table = put_into_individual_table(table, result, columns)
        assert individual_table == {table : [
            {
                "sales_order_id": 8256,
                "created_at": "2024-05-14 16:54:10.308",
                "last_updated": "2024-05-14 16:54:10.308",
                "design_id": 311,
                "staff_id": 1,
                "counterparty_id": 20,
                "units_sold": 99847,
                "unit_price": 3.80,
                "currency_id": 2,
                "agreed_delivery_date": "2024-05-18",
                "agreed_payment_date": "2024-05-18",
                "agreed_delivery_location_id": 6
            }
        ]}


class TestPutObjectInBucket:
    def test_func_puts_obj_in_s3(self,s3_client):
        table = 'testtable'
        put_table = 'test'
        s3_client.create_bucket(
            Bucket='testbucket',
            CreateBucketConfiguration={
                'LocationConstraint' : 'eu-west-2'
            })
        put_object_in_bucket(
            table=table,
            put_table= put_table,
            s3_client=s3_client)
        assert Body.read().decode('utf-8') == text


    