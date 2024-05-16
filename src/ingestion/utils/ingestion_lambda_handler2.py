from connection import connect_to_db, close_connection
from pg8000.native import identifier, literal
from datetime import datetime, date
import datetime
import boto3
import json
import pprint

latest_timestamp = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)

def ingestion_lambda_handler():
    #event, context
    conn = connect_to_db()

    table_names = [
            'sales_order',
            'design',
            'currency',
            'staff',
            'counterparty',
            'address',
            'department',
            'purchase_order',
            'payment_type',
            'payment',
            'transaction'
            ]
    

    def query_updated_table_information(table_names=table_names):

        global latest_timestamp
        table_output = {}

        for table in table_names:
            query = f"""SELECT * 
                        FROM {identifier(table)} 
                        WHERE last_updated > {literal(latest_timestamp)}
                        ORDER BY last_updated ASC
                        LIMIT 2;"""
            result = conn.run(query)
            columns = [col["name"]for col in conn.columns]
            individual_table = {f"{table}": [dict(zip(columns, line)) for line in result]}
            table_output[table] = [dict(zip(columns, line)) for line in result]

            # s3_client = boto3.client("s3")
            
            # now = datetime.datetime.now() 
            # date_time = now.strftime("%m:%d:%Y-%H:%M:%S")
            
            # s3_client.put_object(
            #     Body=json.dumps(str(individual_table)),
            #     Bucket="ldcm-python-test",
            #     Key=f"{table}/{date_time}-{table}-data"
            # )

        get_latest_timestamp = table_output['sales_order'][-1]['last_updated']

        latest_timestamp = get_latest_timestamp

            
    
        return individual_table
    
    result = query_updated_table_information()

    close_connection(conn=conn)
    
    
    return result

ingestion_lambda_handler()
ingestion_lambda_handler()
pprint.pp(ingestion_lambda_handler())


        
# print(datetime.now())
# ingestion_lambda_handler()
# print(datetime.now())