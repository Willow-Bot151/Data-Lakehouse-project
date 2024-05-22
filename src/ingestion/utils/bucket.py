import boto3
import datetime
import json


def delete_bucket():
    s3_client = boto3.resource("s3")
    bucket = s3_client.Bucket("nc-team-reveries-ingestion")
    bucket.delete()


def make_bucket_for_testing():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="nc-team-reveries-ingestion",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def initialise_bucket_with_timestamp():
    s3_client = boto3.client("s3")
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="nc-team-reveries-ingestion",
        Key=f"timestamp",
    )


initialise_bucket_with_timestamp()
