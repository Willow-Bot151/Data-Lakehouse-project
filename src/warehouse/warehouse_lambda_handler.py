from warehouse_utils import (
                             get_aws_secrets,
                             close_connection, 
                             create_dataframe_dictionaries, 
                             connect_to_db_engine, 
                             run_engine_to_insert_database)
import logging

logger = logging.getLogger("Ingestion Lambda Log")
logging.basicConfig()
logger.setLevel(logging.INFO)

def warehouse_lambda_handler(event={}, context=[]):
    logger.info("Running warehouse lambda handler...")

    table_list = ['dim_date', 'dim_design', 'dim_currency', 'dim_counterparty', 'dim_staff', 'dim_location', 'fact_sales_order']
    
    df_dict = create_dataframe_dictionaries(table_list)

    secrets = get_aws_secrets
    engine = connect_to_db_engine(secrets=secrets)
    run_engine_to_insert_database(engine=engine, dict=df_dict)
    
    close_connection()
    logger.info("Loaded data from s3 into warehouse successfully")
