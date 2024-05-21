from src.processing.create_dim_date import create_dim_date
import pytest
import json
import boto3
import os
import pandas as pd
from moto import mock_aws

class TestDateDimensionTable:
    def test_create_dim_date_has_correct_columns(self):
        expected_columns = ['date_id', 'year', 'month', 'day',
                            'day_of_week', 'day_name', 'month_name', 'quarter']
        assert list(create_dim_date().columns) == expected_columns

    def test_create_dim_date_has_correct_data_types(self):
        expected_data_types = {
            'year': 'int32',
            'month': 'int32',
            'day': 'int32',
            'day_of_week': 'int32',
            'day_name': 'object',
            'month_name': 'object',
            'quarter': 'period[Q-DEC]'
        }
        df = create_dim_date()
        for col, data_type in expected_data_types.items():
            assert df[col].dtype == data_type

    def test_create_dim_date_returns_correct_df_data(self):
        