import boto3
from botocore.exceptions import ClientError
#from src.ingestion.utils.test_zip.connection import Connection
import json
from pg8000.native import Connection

#secrets_client=boto3.client('secretsmanager')
#secret_name='team_reveries_PSQL'

def connect_to_db():
    secret_name = 'team_reveries_PSQL'
    region_name = "eu-west-2"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = get_secret_value_response['SecretString']
        #print(secret)
        secret_value = json.loads(secret)
        username = secret_value['username']
        password = secret_value['password']
        database = secret_value['dbname']
        host = secret_value['host']
        port = secret_value['port']
        #print(secret_value)
        return Connection(username, password = password, database = database, host = host, port = port)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

test = connect_to_db()
def close_connection(conn):
    conn.close()