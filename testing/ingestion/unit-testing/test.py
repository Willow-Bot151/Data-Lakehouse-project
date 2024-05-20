from src/ingestion/utils/test_zip/utils.py import 

tables = [
            {"sales_order": {
            "sales_order_id": 8256,
            "created_at": "2024-05-14 16:54:10.308",
            "last_updated": "2024-05-14 17:00:10.308"}},
            {"currency": {
            "sales_order_id": 8256,
            "created_at": "2024-05-14 16:57:10.308",
            "last_updated": "2024-05-14 18:00:10.308"}},
            {"payment": {
            "sales_order_id": 8256,
            "created_at": "2024-05-14 16:57:10.308",
            "last_updated": "2024-05-14 16:00:10.308"}},
            {"staff": {
            "sales_order_id": 8256,
            "created_at": "2024-05-14 16:57:10.308",
            "last_updated": "2024-05-14 19:00:10.308"}},
            {"counterparty": {
            "sales_order_id": 8256,
            "created_at": "2024-05-14 16:57:10.308",
            "last_updated": "2024-05-14 16:00:10.308"}}
            ]

for table in tables:
    potential_timestamp = get_datestamp_from_table(individual_table, table)
        
        if individual_table[table][-1]['last_updated'] > potential_timestamp:
            potential_timestamp = individual_table[table][-1]['last_updated']