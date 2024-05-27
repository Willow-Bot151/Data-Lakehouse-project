from src.processing.create_dim_date import create_dim_date
from src.processing.create_dim_counterparty import create_dim_counterparty
from src.processing.create_dim_location import create_dim_location
from src.processing.create_dim_staff import create_dim_staff
#from src.processing.create_dim_design import create_dim_design
#from src.processing.create_fact_sales import create_fact_sales
from src.processing.processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                list_objects_in_bucket
from src.processing.s3_file_reader import s3_reader_many_files, s3_reader_filtered
import botocore
import datetime
from botocore.exceptions import ClientError
import logging

# processed_table_names = [
#     'dim_date',
#     'dim_staff',
#     'dim_currency',
#     'dim_counterparty',
#     'dim_design',
#     'dim_location',
#     'fact_sales'
#    ]
logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)

def processed_lambda_handler(event={}, context={}):
        try:
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

                # filter_files_by_timestamp, if there are issues look at putting
                # a / between prefix and key and the implications of doing so :(
                

                # now call each dim_table function, return dim_table as df
                dim_date = create_dim_date()
                dim_counter = create_dim_counterparty(df_dict['address'], df_dict['counterparty'])
                dim_staff = create_dim_staff(df_dict['staff'],df_dict['department'])
                dim_loc = create_dim_location(df_dict['address'])
                #dim_design = create_dim_design(df_dict['design'])
                #dim_currency = create_dim_currency(df_dict['currency'])
                #fact_sales = create_facts_sales()

                # output each dim and fact to parquet function
                #write_parquet_file_to_s3(file, s3_client, bucket_name, table_name, date_start, date_end)

                
                #date_time = end_time.strftime("%m:%d:%Y-%H:%M:%S")
                #write_timestamp_to_s3((s3_client, processed_bucket, timestamp)
                response = logger.info("-SUCCESS- Data processed successfully")
                return response
        except Exception:
                response = logger.error("-ERROR- Data processing failed")
                return response


#dict_df=processed_lambda_handler()

