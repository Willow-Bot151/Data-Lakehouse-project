import json
import boto3
import datetime

s3_client = boto3.client("s3")

returned_object = s3_client.get_object(
    Bucket="nc-team-reveries-ingestion",
    Key="staff/--05:21:2024-09:29:34--staff-data"

)

body = returned_object['Body'].read()
        
result = json.loads(body.decode('utf-8'))
        
eval_str_to_dict = eval(result)

print(type(eval_str_to_dict))

"""
2024-05-21 09:29:35        682 address/--05:21:2024-09:29:34--address-data
2024-05-21 09:29:35        647 counterparty/--05:21:2024-09:29:34--counterparty-data
2024-05-21 09:29:35        372 currency/--05:21:2024-09:29:34--currency-data
2024-05-21 09:29:36        499 department/--05:21:2024-09:29:35--department-data
2024-05-21 09:29:35        505 design/--05:21:2024-09:29:34--design-data
2024-05-21 09:29:36        792 payment/--05:21:2024-09:29:35--payment-data
2024-05-21 09:29:36        411 payment_type/--05:21:2024-09:29:35--payment_type-data
2024-05-21 09:29:36        841 purchase_order/--05:21:2024-09:29:35--purchase_order-data
2024-05-21 09:29:35        800 sales_order/--05:21:2024-09:29:34--sales_order-data
2024-05-21 09:29:35        551 staff/--05:21:2024-09:29:34--staff-data
2024-05-21 09:29:36         28 timestamp
2024-05-21 09:29:36        489 transaction/--05:21:2024-09:29:35--transaction-data
"""