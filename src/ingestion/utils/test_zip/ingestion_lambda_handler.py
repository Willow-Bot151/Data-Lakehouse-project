from connection import connect_to_db, close_connection

from utils import put_object_in_bucket, query_updated_table_information, get_datestamp_from_table, get_current_timestamp, put_timestamp_in_s3
#event, context
def ingestion_lambda_handler():

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
    
    dt = get_current_timestamp()

    for table in table_names:
        individual_table = query_updated_table_information(conn, table, dt)
        put_object_in_bucket(table, individual_table)  
        if table == 'sales_order':
            table_to_get_timestamp_from = individual_table 
        
    new_timestamp = get_datestamp_from_table(table_to_get_timestamp_from)
    
    put_timestamp_in_s3(new_timestamp)
    
    close_connection(conn=conn)


test=ingestion_lambda_handler()
print(test)