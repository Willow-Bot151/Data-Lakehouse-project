resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-team-reveries-ingestion"
}

resource "aws_s3_bucket_versioning" "example" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_notification" "ingestion_lambda_trigger" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_eventbridge]
}
# this is to trigger the processing lambda, not sure if it runs concurrently/asynchronously, find out tomorrow!

# resource "aws_s3_bucket_notification" "aws-lambda-trigger" {
# bucket = "${aws_s3_bucket.ingestion_bucket.id}"
# lambda_function {
# lambda_function_arn = "${aws_lambda_function.processing_lambda.arn}"
# events              = ["s3:ObjectCreated:*"]
# filter_prefix       = "file-prefix"
# filter_suffix       = "file-extension"
# }
# }

# resource "aws_lambda_permission" "test" {
# statement_id  = "AllowS3Invoke"
# action        = "lambda:InvokeFunction"
# function_name = "${aws_lambda_function.test_lambda.function_name}"
# principal = "s3.amazonaws.com"
# source_arn = "arn:aws:s3:::${aws_s3_bucket.bucket.id}"
# }
#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_s3_bucket" "processing_bucket" {
  bucket = "nc-team-reveries-processing"
}

resource "aws_s3_bucket_versioning" "processing_bucket_ver" {
  bucket = aws_s3_bucket.processing_bucket.id

  versioning_configuration {
    status = "Enabled"
  }
}
