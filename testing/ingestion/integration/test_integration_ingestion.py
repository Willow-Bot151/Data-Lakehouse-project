import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.utils.test_zip.ingestion_lambda_handler import ingestion_lambda_handler
from src.ingestion.utils.test_zip.connection import connect_to_db, close_connection
from botocore.exceptions import ClientError
from datetime import datetime

@pytest.fixture
def mock_event():
    return {"dummy_event": "data"}

@pytest.fixture
def mock_context():
    return MagicMock()

@pytest.fixture
def mock_conn():
    return MagicMock()

@pytest.fixture
def mock_s3_client():
    return MagicMock()

@pytest.fixture
def mock_table_data():
    return {
        "sales_order": [],
        "design": [],
        "currency": [],
        "staff": [],
        "counterparty": [],
        "address": [],
        "department": [],
        "purchase_order": [],
        "payment_type": [],
        "payment": [],
        "transaction": [],
    }

@pytest.fixture
def mock_latest_timestamp():
    return datetime(2023, 1, 1)

@pytest.fixture
def mock_dt_now():
    return datetime(2024, 5, 23)

@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.connect_to_db")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.init_s3_client")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.get_current_timestamp")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.get_datetime_now")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.add_ts_for_processing_bucket")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.query_updated_table_information")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.convert_datetimes_and_decimals")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.put_object_in_bucket")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.get_datestamp_from_table")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.put_timestamp_in_s3")
@patch("src.ingestion.utils.test_zip.ingestion_lambda_handler.close_connection")
def test_ingestion_lambda_handler_as_a_whole( mock_close_connection,
    mock_put_timestamp_in_s3,mock_get_datestamp_from_table,
    mock_put_object_in_bucket,mock_convert_datetimes_and_decimals,
    mock_query_updated_table_information,mock_add_ts_for_processing_bucket,
    mock_get_datetime_now,mock_get_current_timestamp,
    mock_init_s3_client,mock_connect_to_db,
    mock_event,mock_context,mock_conn,mock_s3_client,
    mock_table_data,mock_latest_timestamp,mock_dt_now):

    mock_connect_to_db.return_value = mock_conn
    mock_init_s3_client.return_value = mock_s3_client
    mock_get_current_timestamp.return_value = mock_latest_timestamp
    mock_get_datetime_now.return_value = mock_dt_now
    mock_convert_datetimes_and_decimals.side_effect = lambda x: x
    mock_query_updated_table_information.return_value = mock_table_data

    ingestion_lambda_handler(mock_event, mock_context)

    # Assertions
    mock_connect_to_db.assert_called_once()
    mock_init_s3_client.assert_called_once()
    mock_get_current_timestamp.assert_called_with(mock_s3_client)
    mock_add_ts_for_processing_bucket.assert_called_with(mock_s3_client, mock_dt_now)
    mock_close_connection.assert_called_once_with(conn=mock_conn)
    mock_put_timestamp_in_s3.assert_called_once_with(mock_latest_timestamp, mock_s3_client)
    mock_get_current_timestamp.assert_called_with(mock_s3_client)
    for table in mock_table_data.keys():
        print('processing table:', table)
        mock_convert_datetimes_and_decimals.assert_any_call(mock_table_data)
        if len(mock_table_data[table]) > 0:
            mock_put_object_in_bucket.assert_any_call(table, mock_table_data, mock_s3_client, "nc-team-reveries-ingestion", mock_dt_now)
            mock_get_datestamp_from_table.assert_any_call(mock_table_data, table)
        else:
            mock_put_object_in_bucket.assert_not_called()
            mock_get_datestamp_from_table.assert_not_called()


# import os
# from pg8000.native import literal, identifier, Connection
# import datetime
# from src.ingestion.utils.test_zip.utils import (
#     convert_datetimes_and_decimals,
#     put_object_in_bucket,
# )
# from moto import mock_aws
# import boto3
# import pytest
# from botocore.exceptions import ClientError
# from botocore.session import get_session
# import json


# @pytest.fixture()
# def connect_to_db():
#     secret_name = "team_reveries_PSQL"
#     region_name = "eu-west-2"
#     session = get_session()
#     client = session.create_client("secretsmanager", region_name=region_name)
#     try:
#         get_secret_value_response = client.get_secret_value(SecretId=secret_name)
#         secret = get_secret_value_response["SecretString"]
#         secret_value = json.loads(secret)
#         username = secret_value["username"]
#         password = secret_value["password"]
#         database = secret_value["dbname"]
#         host = secret_value["host"]
#         port = secret_value["port"]
#         conn = Connection(
#             username, password=password, database=database, host=host, port=port
#         )
#         return conn
#     except ClientError as e:
#         raise ValueError("No connection to DB returned")


# def close_connection(conn):
#     conn.close()


# @pytest.fixture()
# def aws_creds():
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# @pytest.fixture(scope="function")
# def mock_s3_client(aws_creds):
#     with mock_aws():
#         yield boto3.client("s3")


# @pytest.fixture(scope='function')
# def get_sample_data_from_db(connect_to_db):
#     conn = connect_to_db
#     query = f"""SELECT *
#                 FROM {identifier('sales_order')}
#                 WHERE last_updated > {literal(datetime.datetime(2022,1,1,13,20,22))}
#                 ORDER BY last_updated ASC
#                 LIMIT 2;"""
#     result = conn.run(query)
#     columns = [col["name"] for col in conn.columns]
#     individual_table = {"sales_order": [dict(zip(columns, line)) for line in result]}
#     return individual_table


# def test_data_is_json(get_sample_data_from_db, mock_s3_client):
#     mock_s3_client.create_bucket(
#         Bucket="nc-team-reveries-ingestion",
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )

#     table = get_sample_data_from_db
#     json_table = convert_datetimes_and_decimals(table)
#     put_object_in_bucket(
#         "sales_order", json_table, mock_s3_client, "nc-team-reveries-ingestion"
#     )
#     listed_objects = mock_s3_client.list_objects(Bucket="nc-team-reveries-ingestion")
#     returned_object = mock_s3_client.get_object(
#         Bucket="nc-team-reveries-ingestion", Key=listed_objects["Contents"][0]["Key"]
#     )
#     test_var = json.loads(returned_object["Body"].read().decode("utf-8"))

#     assert json_table == test_var
