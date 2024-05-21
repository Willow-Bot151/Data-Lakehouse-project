
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