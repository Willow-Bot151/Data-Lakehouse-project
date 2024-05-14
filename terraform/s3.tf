resource "aws_s3_bucket" "ingestion_code_bucket" {
  bucket_prefix = "nc-team-reveries-ingestion-code-"
}

resource "aws_s3_bucket" "ingestion_data_bucket" {
  bucket_prefix = "nc-team-reveries-ingestion-data-"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.ingestion_code_bucket.bucket
  key = "s3_file_reader/function.zip"
  source = "${path.module}/../function.zip"
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion_data_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_file_reader.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}