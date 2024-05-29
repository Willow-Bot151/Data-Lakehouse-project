import json
import awswrangler as wr
from botocore.exceptions import ClientError
from botocore.session import get_session
import pg8000
from sqlalchemy import create_engine
    
def create_dataframe_dictionaries(table_list):
    df_dict = {}
    for table in table_list:
        key_data_df = wr.s3.read_parquet(path=f's3://nc-team-reveries-processing/{table}/')
        key_data_df.drop_duplicates(inplace=True)
        df_dict[table] = key_data_df
    return df_dict
    
def get_aws_secrets():
        secret_name = "team_reveries_warehouse"
        region_name = "eu-west-2"
        session = get_session()
        client = session.create_client("secretsmanager", region_name=region_name)
        secret_list = {}
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response["SecretString"]
            secret_value = json.loads(secret)
            secret_list['username'] = secret_value["username"]
            secret_list['password'] = secret_value["password"]
            secret_list['database'] = secret_value["dbname"]
            secret_list['host'] = secret_value["host"]
            secret_list['port'] = secret_value["port"]
            return secret_list
        except ClientError:
            raise ClientError("Failure to return secrets from AWS")
            
def connect_to_db_engine(secrets):
    try:
        engine = create_engine(f"postgresql+pg8000://{secrets['username']}:{secrets['password']}@{secrets['host']}:{secrets['port']}/{secrets['database']}")
        return engine
    except ConnectionError:
        raise ConnectionError("Failed to open DB connection")
    
def run_engine_to_insert_database(engine, input_dict):
    with engine.begin() as connection:
            for dataframe_name, dataframe in input_dict.items():
                dataframe.to_sql(name=dataframe_name, con=connection, if_exists='append', index=False)
                success_message = "Succesfully moved dataframe rows to SQL database"
            return success_message
    
def close_connection(conn):
    try:
        conn.dispose()
    except ConnectionError:
        raise ConnectionError("Failed to close DB connection")
    
