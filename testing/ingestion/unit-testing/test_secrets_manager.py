from src.ingestion.utils.test_zip.secrets_manager import new_connect_to_db
import pytest
import os
import boto3
from moto import mock_aws
from pg8000.native import Connection

@pytest.fixture(scope="class")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def secrets_client(aws_creds):
    with mock_aws():
        yield boto3.client("secretsmanager")

def test_new_connect_to_db_psql_connection():
   assert isinstance(new_connect_to_db(secrets_client), Connection)







# class TestGetPsqlSecret:
#     @staticmethod
#     def test_retrieve_returns_correct_mock_secret_data(secrets_client):
#         secrets_client.create_secret(Name="test", SecretString="""{"UserID": "test", "Password": "testpassword"}""")
#         #input='test'
#         result= get_psql_secret(secrets_client)
#         expected = {"UserID": "test", "Password": "testpassword"}
#         expected
#         print(result)
#         assert result == expected

        # assert os.path.exists('secrets_data.txt')
        # file = open('secrets_data.txt', 'r')
        # content = file.read()
        # print(content[0])
        # expected = """{"UserID": "test", "Password": "testpassword"}"""
        # assert content == expected