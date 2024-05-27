import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from src.processing.processing_lambda_handler import processed_lambda_handler
from botocore.exceptions import ClientError

@pytest.fixture
def mock_event():
    return {"dummy_event": "data"}

@pytest.fixture
def mock_context():
    return MagicMock()

@pytest.fixture
def mock_s3_client():
    return MagicMock()

@pytest.fixture
def mock_df():
    return MagicMock()

@pytest.fixture
def mock_timestamps():
    return "01:01:2024-00:00:00", "01:01:2022-00:00:00"

@pytest.fixture
def mock_filtered_files():
    return ["file1", "file2"]

@pytest.fixture
def mock_df_dict():
    return {
        "design": MagicMock(),
        "currency": MagicMock(),
        "staff": MagicMock(),
        "counterparty": MagicMock(),
        "address": MagicMock(),
        "department": MagicMock(),
        "sales_order": MagicMock(),
    }

@patch("src.processing.processing_lambda_handler.init_s3_client")
@patch("src.processing.processing_lambda_handler.read_timestamp_from_s3")
@patch("src.processing.processing_lambda_handler.list_objects_in_bucket")
@patch("src.processing.processing_lambda_handler.filter_files_by_timestamp")
@patch("src.processing.processing_lambda_handler.s3_reader_filtered")
@patch("src.processing.processing_lambda_handler.create_dim_date")
@patch("src.processing.processing_lambda_handler.create_dim_counterparty")
@patch("src.processing.processing_lambda_handler.create_dim_staff")
@patch("src.processing.processing_lambda_handler.create_dim_location")
#@patch("src.processing.processing_lambda_handler.write_parquet_file_to_s3")
def test_processed_lambda_handler(
    mock_create_dim_location,
    mock_create_dim_staff,
    mock_create_dim_counterparty,
    mock_create_dim_date,
    mock_s3_reader_filtered,
    mock_filter_files_by_timestamp,
    mock_list_objects_in_bucket,
    mock_read_timestamp_from_s3,
    mock_init_s3_client,
    #mock_write_parquet_file_to_s3,
    mock_event,
    mock_context,
    mock_s3_client,
    mock_df,
    mock_timestamps,
    mock_filtered_files,
    mock_df_dict
):
    # Setup the mock responses
    mock_init_s3_client.return_value = mock_s3_client
    mock_read_timestamp_from_s3.side_effect = mock_timestamps
    mock_list_objects_in_bucket.return_value = mock_filtered_files
    mock_filter_files_by_timestamp.return_value = mock_filtered_files
    mock_s3_reader_filtered.return_value = mock_df
    mock_create_dim_date.return_value = mock_df
    mock_create_dim_counterparty.return_value = mock_df
    mock_create_dim_staff.return_value = mock_df
    mock_create_dim_location.return_value = mock_df
    #mock_create_dim_design.return_value = mock_df 
    #mock_create_dim_currency.return_value = mock_df    
    #mock_create_fact_sales.return_value = mock_df 
    #mock_write_parquet_file_to_s3.return_value = mock_df  

    processed_lambda_handler(mock_event, mock_context)

    # Assertions
    mock_init_s3_client.assert_called_once()
    assert mock_read_timestamp_from_s3.call_count == 2
    mock_list_objects_in_bucket.assert_called()
    mock_filter_files_by_timestamp.assert_called()
    mock_s3_reader_filtered.assert_called()
    mock_create_dim_date.assert_called_once()
    mock_create_dim_counterparty.assert_called_once()
    mock_create_dim_staff.assert_called_once()
    mock_create_dim_location.assert_called_once()
    #mock_create_dim_design.assert_called_once()   
    #mock_create_dim_currency.assert_called_once()
    #mock_create_fact_sales.assert_called_once()
    #assert mock_write_parquet_file_to_s3.call_count = 7