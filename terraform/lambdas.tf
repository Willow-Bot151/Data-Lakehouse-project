data "archive_file" "lambda" {
  type        = "zip"
  output_file_mode = "0666"
  source_file = "${path.module}/../src/ingestion/utils/sql_utils.py"      #--- has to be changed
  output_path = "${path.module}/../src/ingestion/utils/sql_utils.zip"             #--- has to be changed
}


resource "aws_lambda_function" "ingestion_lambda_handler" {
    function_name = "${var.lambda_name}"
    s3_bucket = aws_s3_bucket.ingestion_bucket.bucket
    s3_key = "ingestion_code/sql_utils.zip"      #--- has to be changed
    role = aws_iam_role.lambda_role.arn
    handler = "ingestion.lambda_handler"       #--- has to be changed
    runtime = var.python_runtime        
    timeout = 60                               # --- might need to be changed for first ingestion pull of all tables
    layers = [aws_lambda_layer_version.requests_layer.arn]
    source_code_hash = data.archive_file.lambda.output_base64sha256    # -- has to be verified
}

resource "aws_lambda_permission" "allow_eventbridge" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda_handler.function_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}








