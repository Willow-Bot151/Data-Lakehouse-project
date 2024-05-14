# resource "aws_lambda_function" "ingestion_lambda_handler" {
#     function_name = "${var.lambda_name}"
#     s3_bucket = aws_s3_bucket.ingestion_code_bucket.bucket
#     s3_key = "ingestion/function.zip"
#     role = aws_iam_role.lambda_role.arn
#     handler = "ingestion.lambda_handler"
#     runtime = var.python_runtime
#     timeout = 60
# #    layers = [aws_lambda_layer_version.requests_layer.arn]
# }

# resource "aws_lambda_permission" "allow_eventbridge" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.ingestion_lambda_handler.function_name
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.scheduler.arn
#   source_account = data.aws_caller_identity.current.account_id
# }