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
