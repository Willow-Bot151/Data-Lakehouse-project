from create_dim_date import create_dim_date
from processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                list_objects_in_bucket
from s3_file_reader import s3_reader_many_files, s3_reader_filtered
import botocore
import datetime


# processed_table_names = [
#     'dim_date',
#     'dim_staff',
#     'dim_currency',
#     'dim_counterparty',
#     'dim_design',
#     'dim_location',
#     'fact_sales'
#    ]


def processed_lambda_handler(event={}, context={}):
        
        ingestion_table_names = [
        'design',
        'currency',
        'staff',
        'counterparty',
        'address',
        'department',
        'sales_order']

        s3_client=init_s3_client()
        ingestion_bucket="nc-team-reveries-ingestion"
        processed_bucket="nc-team-reveries-processing"

        df_dict={}
        end_timestep=read_timestamp_from_s3(ingestion_bucket, 'timestamp_start',s3_client)
        start_timestep=read_timestamp_from_s3(processed_bucket, 'timestamp',s3_client)  

        print('end:',end_timestep)
        print('start:',start_timestep)      

        date_format = "%m:%d:%Y-%H:%M:%S"
        end_time= datetime.datetime.strptime(end_timestep, date_format)
        start_time= datetime.datetime.strptime(start_timestep, date_format)

        for table in ingestion_table_names:
                objects=list_objects_in_bucket(ingestion_bucket,table)
                filtered_files=filter_files_by_timestamp(ingestion_bucket,table,objects, start_time, end_time)
                df=s3_reader_filtered(table,filtered_files)

                df_dict[table]=df

        

        # now call each dim_table function, return dim_table as df
        
        # output to parquet function
        

        # cols_currency=list(df_dict['currency'].columns)
        # print(cols_currency)
        # cols_address=list(df_dict['address'].columns)
        # print(cols_address)
        
        #date_time = end_time.strftime("%m:%d:%Y-%H:%M:%S")
        #write_timestamp_to_s3((s3_client, processed_bucket, timestamp)

        return df_dict


dict_df=processed_lambda_handler()



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
