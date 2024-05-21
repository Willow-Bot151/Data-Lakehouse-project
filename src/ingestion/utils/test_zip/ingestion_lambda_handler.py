from connection import connect_to_db, close_connection

from utils import (
    init_s3_client,
    put_object_in_bucket,
    query_updated_table_information,
    get_datestamp_from_table,
    get_current_timestamp,
    put_timestamp_in_s3,
    convert_datetimes_and_decimals,
)


def ingestion_lambda_handler(event, context):

    conn = connect_to_db()

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
    s3_client = init_s3_client()
    dt = get_current_timestamp(s3_client)
    latest_timestamp = get_current_timestamp(s3_client)

    for table in table_names:
        individual_table = convert_datetimes_and_decimals(
            query_updated_table_information(conn, table, dt)
        )

        if len(individual_table[table]) > 0:
            put_object_in_bucket(
                table, individual_table, s3_client, "nc-team-reveries-ingestion"
            )

        if len(individual_table[table]) > 0:
            potential_timestamp = get_datestamp_from_table(individual_table, table)
            if potential_timestamp > latest_timestamp:
                latest_timestamp = potential_timestamp

    put_timestamp_in_s3(latest_timestamp, s3_client)

    close_connection(conn=conn)
