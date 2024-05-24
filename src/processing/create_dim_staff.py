import pandas as pd
'''
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
    - return modified dataframe
'''

def create_dim_staff(staff_tbl_df,dept_tbl_df):
    dim_staff_column_names = ['staff_id', 'first_name','last_name','email_address']
    dim_staff = staff_tbl_df.loc[:,dim_staff_column_names]
    dim_staff['department_name'] = dept_tbl_df['department_name']
    dim_staff['location'] = dept_tbl_df['location']
    #print(dim_staff)

    #df3 = df1[df['series_name'].isin(df2['series_name'])]
    matching_dept_id = staff_tbl_df['department_id'].isin(dept_tbl_df['department_id'])

    modified_dim_staff = dim_staff.loc[matching_dept_id]
    print(list(modified_dim_staff.columns))
    return modified_dim_staff