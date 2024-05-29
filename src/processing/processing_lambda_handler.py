try:
        from src.processing.create_dim_date import create_dim_date
        from src.processing.create_dim_counterparty import create_dim_counterparty
        from src.processing.create_dim_location import create_dim_location
        from src.processing.create_dim_staff import create_dim_staff
        from src.processing.create_dim_design import create_dim_design
        from src.processing.create_fact_sales import create_fact_sales
        from src.processing.create_dim_currency import create_dim_currency, grab_currency_conversion_info
        from src.processing.processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                        read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                        list_objects_in_bucket
        from src.processing.s3_file_reader import s3_reader_many_files, s3_reader_filtered
except ModuleNotFoundError:
        from create_dim_date import create_dim_date
        from create_dim_counterparty import create_dim_counterparty
        from create_dim_location import create_dim_location
        from create_dim_staff import create_dim_staff
        from create_dim_design import create_dim_design
        from create_fact_sales import create_fact_sales
        from create_dim_currency import create_dim_currency, grab_currency_conversion_info
        from processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                list_objects_in_bucket
        from s3_file_reader import s3_reader_many_files, s3_reader_filtered
        
import botocore
import datetime
from botocore.exceptions import ClientError
import logging


logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)

def processed_lambda_handler(event={}, context={}):
     #   try:
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

                # print('end:',end_timestep)
                # print('start:',start_timestep)      

                date_format = "%m:%d:%Y-%H:%M:%S"
                end_time= datetime.datetime.strptime(end_timestep, date_format)
                start_time= datetime.datetime.strptime(start_timestep, date_format)

                for table in ingestion_table_names:
                        objects=list_objects_in_bucket(ingestion_bucket,table)
                        filtered_files=filter_files_by_timestamp(ingestion_bucket,table,objects, start_time, end_time)
                        df=s3_reader_filtered(table,filtered_files)
                        df_dict[table]=df
                        print(df_dict)
                        logger.info("Processed %s table", table)
                #filter_files_by_timestamp, if there are issues look at putting a / between prefix and key and the implications of doing so :(
                star_schema_tables = dict()
                # now call each dim_table function, return dim_table as df
                star_schema_tables['dim_date'] = create_dim_date()
                logger.info("Dim Date processed.")
                star_schema_tables['dim_counterparty'] = create_dim_counterparty(df_dict['address'], df_dict['counterparty'])
                logger.info("Dim Counterparty processed.")
                star_schema_tables['dim_staff'] = create_dim_staff(df_dict['staff'],df_dict['department'])
                logger.info("Dim Staff processed.")
                star_schema_tables['dim_location'] = create_dim_location(df_dict['address'])
                logger.info("Dim Location processed.")
                star_schema_tables['dim_design'] = create_dim_design(df_dict['design'])
                logger.info("Dim Design processed.")
                star_schema_tables['dim_currency'] = create_dim_currency(df_dict['currency'])
                logger.info("Dim Currency processed.")
                star_schema_tables['fact_sales_order'] = create_fact_sales(df_dict['sales_order'])
                logger.info("Fact Sales Order processed.")

                # output to parquet function
                for table,df in star_schema_tables.items():
                        parquet_data= df_to_parquet(df)
                        write_parquet_file_to_s3(parquet_data, s3_client, processed_bucket, table, start_time, end_time)


                # date_time = end_time.strftime("%m:%d:%Y-%H:%M:%S")
                # write_timestamp_to_s3(s3_client, processed_bucket, date_time)
                response = logger.info("-SUCCESS- Data processed successfully")
                return response
        # except Exception:
        #         response = logger.error("-ERROR- Data processing failed")
        #         return response




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
processed_lambda_handler()