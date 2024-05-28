import pandas as pd
from datetime import datetime, date, time

def fact_sales(sales_order_df):
    copied_sales_order = sales_order_df.copy(deep=True)
    renamed_sales_order = copied_sales_order.rename(columns={"staff_id":"sales_staff_id"})
    renamed_sales_order['created_date'] = renamed_sales_order['created_at'].apply(
        lambda x: datetime.fromisoformat(x).date().isoformat()
        )
    renamed_sales_order['created_time'] = renamed_sales_order['created_at'].apply(
        lambda x: datetime.fromisoformat(x).time().isoformat()
        )
    renamed_sales_order['last_updated_date'] = renamed_sales_order['last_updated'].apply(
        lambda x: datetime.fromisoformat(x).date().isoformat()
        )
    renamed_sales_order['last_updated_time'] = renamed_sales_order['last_updated'].apply(
        lambda x: datetime.fromisoformat(x).time().isoformat()
        )
    filtered_df = renamed_sales_order.drop(columns = ['created_at','last_updated'])
    result_df = filtered_df.reindex([
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ], axis = 1)
    return result_df
    """
    takes: sales_order_df
    returns: fact_sales_df (with foreign keys to dim_tables)
    types:
    "sales_order_id" int [not null]
    "created_date" date [not null]
    "created_time" time [not null]
    "last_updated_date" date [not null]
    "last_updated_time" time [not null]
    "sales_staff_id" int [not null]
    "counterparty_id" int [not null]
    "units_sold" int [not null]
    "unit_price" "numeric(10, 2)" [not null]
    "currency_id" int [not null]
    "design_id" int [not null]
    "agreed_payment_date" date [not null]
    "agreed_delivery_date" date [not null]
    "agreed_delivery_location_id" int [not null]
    raises: error if it cant construct the dataframe

    new columns: times and dates
    renames: staff_id > sales_staff_id
    filter out: created_at and last_updated

    """