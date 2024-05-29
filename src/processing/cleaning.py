schema_requirements = {
    'dim_staff' : {
        'not_null': [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address"
        ],
        'strings' : [
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address"
        ],
        'ints' : [
           "staff_id"
        ],
        'email' : [
           "email_address"
        ]
    }
}
    
# Table "fact_sales_order" {
#   "sales_record_id" SERIAL [pk, increment]
#   "sales_order_id" int [not null]
#   "created_date" date [not null]
#   "created_time" time [not null]
#   "last_updated_date" date [not null]
#   "last_updated_time" time [not null]
#   "sales_staff_id" int [not null]
#   "counterparty_id" int [not null]
#   "units_sold" int [not null]
#   "unit_price" "numeric(10, 2)" [not null]
#   "currency_id" int [not null]
#   "design_id" int [not null]
#   "agreed_payment_date" date [not null]
#   "agreed_delivery_date" date [not null]
#   "agreed_delivery_location_id" int [not null]
# }

# Table dim_date as DT {
#   date_id date [pk, not null]
#   year int [not null]
#   month int [not null]
#   day int [not null]
#   day_of_week int [not null]
#   day_name varchar [not null]
#   month_name varchar [not null]
#   quarter int [not null]
# }


#   staff_id int [pk, not null]
#   first_name varchar [not null]
#   last_name varchar [not null]
#   department_name varchar [not null]
#   location varchar [not null]
#   email_address email_address [not null]


# Table dim_location as LOC {
#   location_id int [pk, not null]
#   address_line_1 varchar [not null]
#   address_line_2 varchar
#   district varchar
#   city varchar [not null]
#   postal_code varchar [not null]
#   country varchar [not null]
#   phone varchar [not null]
# }

# Table dim_currency as C {
#   currency_id int [pk, not null]
#   currency_code varchar [not null]
#   currency_name varchar [not null]
# }

# Table dim_design as D{
#   design_id int [pk, not null]
#   design_name varchar [not null]
#   file_location varchar [not null]
#   file_name varchar [not null]
# }

# Table dim_counterparty as CO {
#   counterparty_id int [pk, not null]
#   counterparty_legal_name varchar [not null]
#   counterparty_legal_address_line_1 varchar [not null]
#   counterparty_legal_address_line2 varchar
#   counterparty_legal_district varchar
#   counterparty_legal_city varchar [not null]
#   counterparty_legal_postal_code varchar [not null]
#   counterparty_legal_country varchar [not null]
#   counterparty_legal_phone_number varchar [not null]


# }


def clean_data(table_name, table_df, schema_requirements):
    new_df = table_df.copy(deep=True)
    if schema_requirements[table_name]['not_null']:
        for column in schema_requirements[table_name]['not_null']:
            