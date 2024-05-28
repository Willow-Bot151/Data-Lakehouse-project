"""
We have our parquet tables
we need to access these by keys, and worry about duplications maybe, primary id might prevent this or error it
we need a func that will organise them for our insert statement


list of our keys
    -botocore
will loop through
check contents in each iteration

for table in tables
access warehouse here*
    -secret manager
    -pg8000

insert into 'table'

if table == sales_order
sales_order_insert_func()

for row in salesorder:
            rest_area_id = conn.run(
                "SELECT area_id FROM areas WHERE area_name = :area_name",
                area_name=row['area_name']
            )
            conn.run(
                "INSERT INTO restaurants \
                    (restaurant_name, area_id, cuisine, website) \
                VALUES \
                    (:restaurant_name, :area_id, :cuisine, :website);",
                restaurant_name=row["restaurant_name"],
                area_id=rest_area_id[0][0],
                cuisine=row["cuisine"],
                website=row["website"],


"""