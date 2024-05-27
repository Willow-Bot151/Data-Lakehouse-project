# from create_dim_date import create_dim_date
# from processing_utils import df_to_parquet, init_s3_client, write_parquet_S3, write_timestamp_to_s3, read_timestamp_from_s3

# # ingestion_table_names = [
# #         'design',
# #         'currency',
# #         'staff',
# #         'counterparty',
# #         'address',
# #         'department'
# #         'sales_order',
# #         ]


# # processed_table_names = [
# #     'dim_date',
# #     'dim_staff',
# #     'dim_currency',
# #     'dim_counterparty',
# #     'dim_design',
# #     'dim_location',
# #     'fact_sales'
# #    ]


#    #print("Received event: " + json.dumps(event, indent=2))
# #     # Get the object from the event and show its content type
# #     bucket = event['Records'][0]['s3']['bucket']['name']
# #     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
# #     try:
# #         response = s3.get_object(Bucket=bucket, Key=key)
# #         print("CONTENT TYPE: " + response['ContentType'])
# #         return response['ContentType']
# #     except Exception as e:
# #         print(e)
# #         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
# #         raise e


# # def initialise_bucket_with_timestamp():
# #     s3_client = boto3.client("s3")
# #     dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
# #     s3_client.put_object(
# #         Body=json.dumps(dt.isoformat()),
# #         Bucket="nc-team-reveries-ingestion",
# #         Key=f"timestamp",
# #     )


# def test_processing_lambda(event={}, context={}):
#     s3_client = init_s3_client()
    
#     dim_date = create_dim_date()
#     transform_to_parquet = df_to_parquet(dim_date)
#     end_timestamp = read_timestamp_from_s3("nc-team-reveries-ingestion", 'timestamp',s3_client=s3_client)
#     write_timestamp_to_s3(s3_client, end_timestamp)
#     write_parquet_S3(s3_client, transform_to_parquet,"dim_date")
    
# test = test_processing_lambda()
# print(test)