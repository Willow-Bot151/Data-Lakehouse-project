data "archive_file" "dependancies" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../layer"
  output_path = "../python.zip"
}

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
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../python.zip"
}

resource "aws_lambda_function_event_invoke_config" "lambda_invoke_config" {
  function_name                = aws_lambda_function.ingestion_lambda.function_name
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
  qualifier     = "$LATEST"
}

#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

data "archive_file" "processing_lambda_data" {
  type = "zip"
  output_file_mode = "0666"
  source_dir = "../src/processing/"
  output_path = "../processing_deploy.zip"
}



resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processing_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingestion_bucket.arn
}

resource "aws_lambda_function" "processing_lambda" {
    function_name = "processing_lambda"
    filename = "../processing_deploy.zip"
    role = aws_iam_role.lambda_role.arn 
    handler = "processing_lambda_handler.test_processing_lambda" #change me
    runtime = var.python_runtime        
    timeout = 60                 # --- consider time taken of ingestion lambda and when this one is triggered
    source_code_hash = data.archive_file.processing_lambda_data.output_base64sha256
    layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:12",aws_lambda_layer_version.processing_dependancies_layer.arn] #think about account name/id if this doesn't work
    
}


resource "aws_lambda_function_event_invoke_config" "processing_lambda_invoke_config" {
  function_name                = aws_lambda_function.processing_lambda.function_name #change me
  maximum_event_age_in_seconds = 60
  maximum_retry_attempts       = 0
  qualifier     = "$LATEST"
}


# resource "aws_lambda_permission" "allow_eventbridge_trigger_processing" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.ingestion_lambda.function_name #change me
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.ingestion_scheduler.arn #change me?
#   source_account = data.aws_caller_identity.current.account_id
# }

#layer reference for main body if we need extra depedencies
#aws_lambda_layer_version.processing_dependencies_layer.arn


data "archive_file" "processing_dependencies" {
  type        = "zip"
  output_file_mode = "0666"
  source_dir = "../layer_processing"
  output_path = "../processing_requirements.zip"       
}


resource "aws_lambda_layer_version" "processing_dependencies_layer" {
  layer_name = "processing_dependancies_layer"
  compatible_runtimes = [var.python_runtime]
  compatible_architectures = ["x86_64", "arm64"]
  filename = "../processing_requirements.zip"
}

# resource "aws_lambda_permission" "processing_lambda_invoke" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.processing_lambda.function_name 
#   principal = "s3.amazonaws.com"
#   source_arn = aws_s3_bucket.ingestion_bucket.arn #change me?
#   source_account = data.aws_caller_identity.current.account_id
# }