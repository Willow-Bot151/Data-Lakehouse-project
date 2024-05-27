import pandas as pd

def create_dim_design(design_df):
    print(design_df.columns)
    required_columns = [
            "design_id",
            "design_name",
            "file_location",
            "file_name"
        ]
    dim_design_df = design_df.filter(required_columns)
    return dim_design_df