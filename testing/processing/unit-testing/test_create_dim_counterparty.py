import pytest
import json
from src.processing.create_dim_counterparty import create_dim_counterparty
import pandas as pd

@pytest.fixture()
def counterparty_test_data():
    with open("./data/table_json_data_fake_ingestion_data/fakedata.json", "r") as f:
        return json.load(f)

@pytest.fixture()
def create_test_data_df(counterparty_test_data):
    dfs = dict()
    for k, v in counterparty_test_data.items():
        dfs[k] = pd.DataFrame(v)
    return dfs

@pytest.fixture()
def create_address_df(create_test_data_df):
    address_df = create_test_data_df['address']
    return address_df

@pytest.fixture()
def create_counterparty_df(create_test_data_df):
    counterparty_df = create_test_data_df['counterparty']
    return counterparty_df

class TestCreateDimCounterparty:
    def test_dim_counterparty_function_returns_a_df(self, create_address_df, create_counterparty_df):
        result = create_dim_counterparty(
            address_df=create_address_df,
            counterparty_df=create_counterparty_df
            )
        assert isinstance(result,pd.DataFrame)
    def test_dim_counterparty_func_has_star_schema_columns(self, create_address_df, create_counterparty_df):
        expected = [
            'counterparty_id',
            'counterparty_legal_name',
            'counterparty_legal_address_line_1',
            'counterparty_legal_address_line2',
            'counterparty_legal_district',
            'counterparty_legal_city',
            'counterparty_legal_postcode',
            'counterparty_legal_country',
            'counterparty_legal_phone_number'
            ]
        result = create_dim_counterparty(
            address_df=create_address_df,
            counterparty_df=create_counterparty_df
            )
        assert result.columns == expected
    """
    function must:
        return df of dim data
            must be a df
            must have the star schema columns
                compare list of columns and expected
            input dfs unchanged
            must have expected values
            must not lose data
                compare len of df_out to min(len of dfs_in)
        take 
    """
class TestCleanMyDF:
    def test_clean_data(self):
        non_nulls = ['counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_city',
        'counterparty_legal_postcode',
        'counterparty_legal_country',
        'counterparty_legal_phone_number']
    
        strings = ['counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_address_line2',
        'counterparty_legal_district',
        'counterparty_legal_city',
        'counterparty_legal_postcode',
        'counterparty_legal_country',
        'counterparty_legal_phone_number']
        ints = ['counterparty_id']