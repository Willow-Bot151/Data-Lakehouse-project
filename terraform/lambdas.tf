data "archive_file" "lambda" {
  type        = "zip"
  output_file_mode = "0666"
  source_file = "${var.source_file}"   #--- has to be changed
  output_path = "${var.output_path}"          #--- has to be changed
}

# Create a lambda function
resource "aws_lambda_function" "ingestion_lambda_handler" {
    function_name = "${var.lambda_name}"
#    filename      = data.archive_file.lambda.output_path
    s3_bucket = aws_s3_object.lambda_code.bucket
    s3_key = aws_s3_object.lambda_code.key    #--- has to be changed
    role = aws_iam_role.lambda_role.arn
    handler = "ingestion.lambda_handler"       #--- has to be changed
    runtime = var.python_runtime        
    timeout = 60                               # --- might need to be changed for first ingestion pull of all tables
    layers = [aws_lambda_layer_version.ingestion_layer.arn]
    source_code_hash = data.archive_file.lambda.output_base64sha256    # -- has to be verified
}

resource "aws_lambda_permission" "lambda_invoke" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda_handler.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}


resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda_handler.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.ingestion_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}

resource "aws_lambda_layer_version" "ingestion_layer" {
  layer_name = "ingestion_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
  s3_key = "ingestion_code/sql_utils.zip"  
}





