<h2> Breakdown of python code behaviours for section between ingestion and processing buckets</h2>

**Read files from S3 bucket -S3_reader.py**
- take a table name as a parameter
- read json format into pandas dataframe (calls function below)
- AWS wrangler or boto3 for listing and reading into one place
- Be able to read files that were written between specific timestamps (function 3)


**Function to write last date to parameter store - parameter_store_writer.py**

**Function to read last date wrtten to parameter store - parameter_store_reader.py**

 **Function to read json format into pandas dataframe - json_to_pd.py**
  - takes json format and converts to dataframe


**Function to read files between two timestamps - timestamp_range_reader.py**
- list all files between two timestamps for a given folder/key
- read into a pandas dataframe

**function write to s3  - s3_writer.py** 
 - write formatted parquet table to processed bucket


<h3>Dimension Functions</h3>

(1) **dim_staff function   - dim_staff.py**

    Final Columns needed :  
        staff_id         Primary Key to be created
        first_name       from staff table
        last_name        from staff table
        department_name  from dept table using dept ID to join tables
        location         from dept table using dept ID to join tables as above
        email_address    from staff


    - read data from staff table
    - read data from dept table
    - extraction of data into PD and join tables
    - drop some columns and adding more to final dataframe
    - output columns as above to parquet


(2) **dim_currency function -dim_currency.py**

    final columns needed:  
        currency_id       Primary key to be created
        currency_code     from currency table
        currency_name     Need to get currency code from currency table and look up online?

    - read data from currency data
    - convert to PD
    - make dictionary with currency code and name (prepared from online lookup?)
       - library called currency codes: https://pypi.org/project/currency-codes/
       - might depend on how many currencies we are dealing with
       - **may need to create another function to lookup code using the package above**
    - output columns to parquet


(3) **dim_counterparty function - dim_counterparty.py**

    final columns needed:
        counterparty_id                     primary key to be created
        counterparty_legal_name             counterparty table
        counterparty_legal_address_line_1   from address table joined using address id
        counterparty_legal_address_line2    from address table joined using address id
        counterparty_legal_dictrict         from address table joined using address id
        counterparty_legal_city             from address table joined using address id
        counterparty_legal_postal_code      from address table joined using address id
        counterparty_legal_country          from address table joined using address id
        counterparty_legal_phone_number     counterparty table

    - read counterparty data
    - read address data
    - convert both to PD and join the tables
    - massage columns to final table
    - write to parquet
    - write to S3 processed bucket


(4) **dim_design function   - dim_design.py**
    final columns needed:
        design_id               primary key to be added
        design_name             design table
        file_location           design table
        file_name               design table

    - read design table data
    - convert to PD
    - massage columns to final table (need to drop some coumns)
    - write to parquet
    - write to s3

(5) **dim_location function  - dim_location.py**

    final columns needed:
        location_id             primary key to be added
        address_line_1          from address table
        address_line_2          from address table
        district                from address table
        city                    from address table
        postal_code             from adress table
        country                 from address table
        phone                   from address table

    - read data from address table
    - convert to PD
    - massage columns to drop some columns from PD
    - write to parquet
    - write to S3

(6) **dim_date function  - dim_date.py**

    final columns needed:
        date_id             primary key to be added?
        year                
        month
        day
        day_of_week
        day_name
        month_name
        quarter     (ie Q1, Q2, Q3, Q4)


    - we think this table creates a datetime range from very first data to latest date
    - possibly create a range of data from 2022-01-01 to 2024-05-31? 
    - for each date, extract year.month,day, day_of_week, day_name,month_name,quarter

    - No data to read - just create a large dataframe from date range
    - use datetime and pandas inbuilt date functionality to extract other columns dt.year etc
    - write to parquet
    - write to S3


<h3>Fact Function </h3>

**fact_sales_order  - fact_sales.py**

    final_columns needed:

        Sales_record_id     primary id to be created? 
        Sales_order_id      from sales order table
        created_date        from sales order table (created_at_date col - date element part)       joins dim_date on date_id
        created_time        from sales order table (created_at_date col - time element part)        
        last_updated_date   from sales order table ( ast_updated column - date part)               joins dim_date on date_id
        last_updated_time   from sales order table (last_updated column - time part)
        sales_staff_id      from dim_staff table                                                   joins dim_staff on staff ID
        counterparty_id     from dim_counterparty table                                            joins dim_counterparty on ID
        units_sold          from sales order table
        unit_price          from sales order table
        currency_id         from dim_currency                                                      joins on currency_id
        design_id           from dim_design table                                                  joins on design_id
        agreed_payment_date from sales order                                                       joins on date_id
        agreed_delivery_date from sales order table                                                joins on date_id
        agreed_delievery_location_id    from dim_location                                          joins on location_id



**Lambda handler to run all of the above code  - processed_lambda_handler.py**

- loops over all MVP table functions to write parquret to processed bucket