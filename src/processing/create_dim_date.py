import pandas as pd

def create_dim_date():

    date_range = pd.date_range(start="2022-01-01", end="2024-05-21", freq="D")
    dim_date = pd.date_range(start='2022-01-01', end='2024-05-21', freq='D')
    
    dim_date = pd.DataFrame(date_range, columns=['date_id'])
    dim_date['year'] = dim_date['date_id'].dt.year
    dim_date['month'] = dim_date['date_id'].dt.month
    dim_date['day'] = dim_date['date_id'].dt.day
    dim_date['day_of_week'] = dim_date['date_id'].dt.weekday
    dim_date['day_name'] = dim_date['date_id'].dt.strftime('%A')
    dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['date_id'].dt.to_period('Q')
    
    return dim_date

def update_dim_date():
    pass