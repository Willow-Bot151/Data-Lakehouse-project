from src.processing.processing_utils import df_normalisation
import pytest
import json
import pandas as pd

@pytest.fixture
def test_data_to_df():
    with open('testing/processing/test_data.json') as f:
        test_data= json.load(f)
    df = pd.DataFrame(test_data)
    return df

class TestDfNorm:
    def test_df_normalisation_does_what_its_says(self,test_data_to_df):
        test_df=test_data_to_df
        remodelled_df=df_normalisation(test_df,'transaction')
        assert isinstance(remodelled_df,pd.DataFrame)
        assert remodelled_df.shape == (10,6)
        assert list(remodelled_df.columns) == ['transaction_id', 'transaction_type', \
                                         'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']
        assert remodelled_df['created_at'][0] == '2022-11-03T14:20:52.186000'

