# data "archive_file" "dependancies" {
#   type = "zip"
#   output_file_mode = "0666"
#   source_dir = "../src/ingestion/utils/test_zip/"
#   output_path = "../src/ingestion/utils/test_zip/my_venv/lib/python3.11/site-packages/my_deployment_package.zip"
# }

data "archive_file" "ingestion_lambda_file" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../src/ingestion/utils/test_zip"   
  output_path = "../terraform/deploy.zip"          
}

resource "aws_lambda_function" "ingestion_lambda" {
    function_name = "ingestion_lambda"
    filename = "deploy.zip"
    role = aws_iam_role.lambda_role.arn
    handler = "ingestion_lambda_handler.ingestion_lambda_handler"       
    runtime = var.python_runtime        
    timeout = 60                               # --- might need to be changed for first ingestion pull of all tables
    source_code_hash = data.archive_file.ingestion_lambda_file.output_base64sha256
    layers = [aws_lambda_layer_version.dependancies_layer.arn]
}


resource "aws_lambda_permission" "lambda_invoke" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}


resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.ingestion_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_lambda_layer_version" "dependancies_layer" {
  layer_name = "dependancies_layer"
  compatible_runtimes = [var.python_runtime]
  filename = "../python.zip"
}

resource "aws_lambda_function_event_invoke_config" "lambda_invoke_config" {
  function_name                = aws_lambda_function.ingestion_lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
}


