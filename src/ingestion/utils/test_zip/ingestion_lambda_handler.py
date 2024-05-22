from connection import connect_to_db, close_connection
from datetime import datetime
from utils import (
    init_s3_client,
    put_object_in_bucket,
    query_updated_table_information,
    get_datestamp_from_table,
    get_current_timestamp,
    put_timestamp_in_s3,
    convert_datetimes_and_decimals,
)
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def ingestion_lambda_handler(event, context):
    logger.info("Ingestion process beginning")
    try:
        conn = connect_to_db()
    except ClientError as e:
        logger.error("-!ERROR!- No connection to DB returned")
        raise e("No connection to DB returned")
    table_names = [
        "sales_order",
        "design",
        "currency",
        "staff",
        "counterparty",
        "address",
        "department",
        "purchase_order",
        "payment_type",
        "payment",
        "transaction",
    ]
    try:
        s3_client = init_s3_client()
    except Exception as e:
        logger.error("-!ERROR!- S3 client failed")
        raise e("S3 client failed")


    try:
        dt = get_current_timestamp(s3_client)
        latest_timestamp = get_current_timestamp(s3_client)
    except Exception as e:
        logger.error(
        """-!ERROR!- An error occured accessing the timestamp from s3 bucket. 
                        Please check that there is a timestamp and it's format is correct""")
        raise e("An error occured accessing the timestamp from s3 bucket. Please check that there is a timestamp and it's format is correct")

    for table in table_names:
        try:
            individual_table = convert_datetimes_and_decimals(query_updated_table_information(conn, table, dt))
        except Exception as e:
            logger.error("-!ERROR!- An error occurred in DB query")
            raise e("An error occurred in DB query")
        if len(individual_table[table]) > 0:
            try:
                put_object_in_bucket(table, individual_table, s3_client, "nc-team-reveries-ingestion")
            except Exception as e:
                logger.error("-!ERROR!- Failed to put object in bucket")
                raise e("Failed to put object in bucket")
        if len(individual_table[table]) > 0:
            try:
                potential_timestamp = get_datestamp_from_table(individual_table, table)
            except Exception as e:
                logger.error("-!ERROR!- couldn't retrieve datestamp from table")
                raise e("couldn't retrieve datestamp from table")
            dt_potential_timestamp = datetime.fromisoformat(potential_timestamp)
            if dt_potential_timestamp > latest_timestamp:
                latest_timestamp = dt_potential_timestamp
                
    try:
        put_timestamp_in_s3(latest_timestamp, s3_client)
    except Exception as e:
        logger.error("-!ERROR!- failed to put timestamp in bucket")
        raise e("failed to put timestamp in bucket")
    logger.info("-!STARTPROCESSING!- Ingestion Process is complete.")
    close_connection(conn=conn)
