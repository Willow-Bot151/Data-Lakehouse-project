import pytest
from src.ingestion.utils.sql_utils import *


@pytest.fixture
def get_table_names():
    tables = [
        "counterparty",
        "currency",
        "department",
        "design,staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]
    return tables


def test_select_head_util_returns_dict(get_table_names):
    for table in get_table_names:
        assert isinstance(select_head_from_given_table(table), dict)


def test_select_head_util_returns_100_rows(get_table_names):
    for table in get_table_names:
        assert len(select_head_from_given_table(table)) == 100

import pytest
from src.ingestion.utils.sql_utils import *
import pytest
from src.ingestion.utils.utilities import get_current_timestamp, get_datestamp_from_table, get_datetime_now, put_into_individual_table, put_object_in_bucket, query_updated_table_information
import datetime
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from moto import mock_aws
import boto3

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

# def test_select_head_util_returns_dict(get_table_names):
#     for table in get_table_names:
#         assert isinstance(select_head_from_given_table(table),dict)
# def test_select_head_util_returns_100_rows(get_table_names):
#     for table in get_table_names:
#         assert len(select_head_from_given_table(table)) == 100
@pytest.fixture
def mock_boto3_client():
    with patch("boto3.client") as mock_client:
        yield mock_client

class TestGetCurrentTimestamp:
    def test_get_currrent_timestamp_returns_a_datetime_object(self):
        result = get_current_timestamp()
        assert isinstance(result, datetime.datetime)

    def test_get_current_timestamp_returns_expected_dt_when_mocked(self,mock_boto3_client):
        mock_response = Mock()
        mock_response.read.return_value = b'"2024-05-16T12:00:00"'
        # Mocking the get_object method of s3_client to return the mock response
        mock_boto3_client.return_value.get_object.return_value = {'Body': mock_response}
        expected_timestamp = datetime.datetime.fromisoformat("2024-05-16T12:00:00")
        result = get_current_timestamp()
        #print(result)
        assert result == expected_timestamp

    def test_get_current_timestamp_returns_error_message(self,mock_boto3_client):
        # Mocking the boto3 client to raise an exception
        mock_boto3_client.side_effect = ClientError({}, "operation_name")
        result = get_current_timestamp()
        assert result is None

    def test_get_current_timestamp_returns_error_message_when_something_goes_wrong(self,mock_boto3_client):
        expected_error_message = "An error occurred:"
        mock_boto3_client.side_effect = Exception("Test Error Message")
        with pytest.raises(Exception) as exc_info:
            get_current_timestamp()
            assert str(exc_info.value) == expected_error_message

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
    def test_query_updated_table_information_empty_result(self,mock_conn):
        mock_conn.run.return_value = []
        dt = "2024-05-16T11:00:00"
        table = "example_table"
        output_table = query_updated_table_information(mock_conn, table, dt)
        assert len(output_table[table]) == 0

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
        assert len(return_table[table]) < 10

    def test_query_updated_table_information_malformed_data(self,mock_conn):
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
    fake_data = {
    'sales_order': [
        {'last_updated': '2022-03-12 19:24:01:377'},
        {'last_updated': '2024-05-14 16:45:09.72'},
        {'last_updated': '2024-05-14 16:54:10.308'}
    ]
}
    result = get_datestamp_from_table(fake_data)
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
# @pytest.fixture
# def s3_bucket(s3, bucket_name):
#   s3.create_bucket(Bucket=bucket_name)
#   yield
# class TestPutObjectInBucket:
#     #https://getbetterdevops.io/how-to-mock-aws-services-in-python-unit-tests/
#     def test_put_object(self, s3, bucket_name='testing', key='testing', body='testing'):
#         with s3_bucket(s3, bucket_name):
#             s3.put_object(Bucket=bucket_name, Key=key, Body=body)
#             object_response = s3.get_object(Bucket=bucket_name, Key=key)
#             assert object_response['Body'].read().decode() == body
    # def test_function_puts_object_into_s3bucket(self, mock_boto3_client):
    #     bucket_name = 'testing'
    #     key = 'testing'
    #     body = 'testing'
    #     mock_response = Mock()
    #     mock_boto3_client.return_value.put_object.return_value = mock_response
    #     put_object_in_bucket()
@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")
        
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
    # def test_write_to_s3_success_message(self,s3_client):
    #     bucket_name = 'test-bucket'
    #     file_name = '/tests/test_sonnet.txt'
    #     path_to_file='./tests/sonnet18.txt'
    #     s3_client.create_bucket(
    #         Bucket=bucket_name,
    #         CreateBucketConfiguration={
    #             'LocationConstraint' : 'eu-west-2'
    #         })
    #     response = write_file_to_s3(
    #         path_to_file=path_to_file,
    #         bucket_name= bucket_name,
    #         object_key= file_name,
    #         s3_client=s3_client)
    #     assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def get_current_timestamp():
    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(
                Bucket="ldcm-python-test",
                Key="timestamp"
        )
        print(response)
        body = response['Body'].read()
        dt_str = json.loads(body.decode('utf-8'))
        dt = datetime.datetime.fromisoformat(dt_str)
        return dt
    except Exception as e:
       # Handle any exceptions that occur during the operation
        print("An error occurred:", e)
        return None

def query_updated_table_information(conn, table, dt):
    try:
        query = f"""SELECT *
                    FROM {identifier(table)}
                    WHERE last_updated > {literal(dt)}
                    ORDER BY last_updated ASC
                    LIMIT 10;"""
        result = conn.run(query)
        columns = [col["name"] for col in conn.columns]
        output_table = put_into_individual_table(table, result, columns)
        return output_table
    except Exception as e:
        print("An error occurred in DB query:", e)
        return None