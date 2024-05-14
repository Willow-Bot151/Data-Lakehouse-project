import pytest
from src.ingestion.utils.sql_utils import *


@pytest.fixture
def get_table_names():
    tables = [
        "counterparty",
        "currency",
        "department",
        "design,staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]
    return tables


def test_select_head_util_returns_dict(get_table_names):
    for table in get_table_names:
        assert isinstance(select_head_from_given_table(table), dict)


def test_select_head_util_returns_100_rows(get_table_names):
    for table in get_table_names:
        assert len(select_head_from_given_table(table)) == 100
