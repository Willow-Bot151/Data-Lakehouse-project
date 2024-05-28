import json
import datetime
import botocore.session
from pg8000.native import identifier, literal
from decimal import Decimal


def init_s3_client():
    try:
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        return s3_client
    except Exception:
        raise ConnectionRefusedError("Failed to connect to s3 client")


def get_current_timestamp(s3_client):
    try:
        response = s3_client.get_object(
            Bucket="nc-team-reveries-ingestion", Key="timestamp"
        )

        body = response["Body"].read()
        dt_str = json.loads(body.decode("utf-8"))

        dt = datetime.datetime.fromisoformat(dt_str)
        return dt
    except Exception:
        raise TypeError(
            """An error occured accessing the timestamp from s3 bucket. 
                        Please check that there is a timestamp and it's format is correct"""
        )


def query_updated_table_information(conn, table, dt):
    try:
        query = f"""SELECT *
                    FROM {identifier(table)}
                    WHERE last_updated > {literal(dt)}
                    ORDER BY last_updated ASC;"""
        result = conn.run(query)

        columns = [col["name"] for col in conn.columns]

        output_table = put_into_individual_table(table, result, columns)

        return output_table
    except Exception as e:
        print("An error occurred in DB query:", e)
        return None


def put_into_individual_table(table, result, columns):
    individual_table = {table: [dict(zip(columns, line)) for line in result]}

    return individual_table


def get_datestamp_from_table(individual_table, table_name):
    try:
        timestamp = individual_table[table_name][-1]["last_updated"]
        return timestamp
    except Exception as e:
        raise e


def get_datetime_now():
    now = datetime.datetime.now()
    date_time = now.strftime("%m:%d:%Y-%H:%M:%S")
    return date_time


def put_object_in_bucket(table, put_table, s3_client, bucket_name, dt_now):
    s3_client.put_object(
        Body=json.dumps(put_table),
        Bucket=bucket_name,
        Key=f"{table}/--{dt_now}--{table}-data",
    )


def put_timestamp_in_s3(timestamp, s3_client):
    try:
        dt = s3_client.put_object(
            Body=json.dumps(timestamp.isoformat()),
            Bucket="nc-team-reveries-ingestion",
            Key=f"timestamp",
        )
        return dt
    except Exception:
        raise RuntimeError("Check connection and formatting of timestamp")


def initialise_bucket_with_timestamp(s3_client):
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    response = s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="nc-team-reveries-ingestion",
        Key=f"timestamp",
    )
    return response


def convert_datetimes_and_decimals(unconverted_json):

    for k, v in unconverted_json.items():
        for entry in v:
            for m, n in entry.items():
                if isinstance(n, datetime.datetime):
                    entry[m] = n.isoformat()
                elif isinstance(n, Decimal):
                    entry[m] = str(n)
    return unconverted_json


def add_ts_for_processing_bucket(s3_client, dt_now):
    response = s3_client.put_object(
        Body=dt_now, Bucket="nc-team-reveries-ingestion", Key=f"timestamp_start"
    )
    return response


def initialise_process_bucket_with_timestamp(s3_client):
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1)
    date_time = dt.strftime("%m:%d:%Y-%H:%M:%S")
    response = s3_client.put_object(
        Body=date_time,
        Bucket="nc-team-reveries-processing",
        Key=f"timestamp",
    )
    return response
