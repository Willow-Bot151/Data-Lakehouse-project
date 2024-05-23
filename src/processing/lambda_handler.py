

# ingestion_table_names = [
#         'design',
#         'currency',
#         'staff',
#         'counterparty',
#         'address',
#         'department'
#         'sales_order',
#         ]


# processed_table_names = [
#     'dim_date',
#     'dim_staff',
#     'dim_currency',
#     'dim_counterparty',
#     'dim_design',
#     'dim_location',
#     'fact_sales'
#    ]


   #print("Received event: " + json.dumps(event, indent=2))
#     # Get the object from the event and show its content type
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     try:
#         response = s3.get_object(Bucket=bucket, Key=key)
#         print("CONTENT TYPE: " + response['ContentType'])
#         return response['ContentType']
#     except Exception as e:
#         print(e)
#         print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
#         raise e


# def initialise_bucket_with_timestamp():
#     s3_client = boto3.client("s3")
#     dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
#     s3_client.put_object(
#         Body=json.dumps(dt.isoformat()),
#         Bucket="nc-team-reveries-ingestion",
#         Key=f"timestamp",
#     )


def processing_lambda(event={}, context={}):
    

    '''
    (1) Add logging! 

    (2) A time range needs to be defined to filter the ingestion files
         - Get current timestamp from ingestion bucket - ie this form the 'end' timestamp of range for grabbing files
         - ingestion bucket timestamp  will be saved to processed bucket to become the start timestamp 
            for next run
        
            
     (3) make processed bucket/timestamp
         - initialise counter =0 in code to create timestamp in processed bucket
         - save ingestion bucket timestamp to processed bucket/timestamp
         - set flag of  count = 1 so bucket is not remade again
        
        if count = 0 - get everything from bucket
          start = 2022-01-01 
          end = timestamp read from ingestion bucket/timestamp

          
     (4) Loop through the ingestion table names that are needed to create MVP processed version
         Read data in time range into a dataframe eg df_design (using fucntions in s3_file_reader)

         
     (5) create star scheme tables in individual functions     
         - dim_date table is exception unless we add updating to it
           for simplicity initially end date set to end of course 31-05-2024

         EXAMPLE:  create function create_dim_staff(df_staff,df_dept)
            will transform passed dfs into requested form (dropping cols and merging etc) 
            returns both a parquet form of star schema table and dataframe of table (for use in facts table)

        ** dim tables first
           need df versions of processed tables for passing into fact table
           create_facts(df_dim_date,df_dim_loc)


      (6) overwrite timestamp in processed bucket/timestamp  with timestamp coming from ingestion bucket
          named 'start' somewhere in timestamp 
    
    '''
    pass