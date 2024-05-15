
# resource "aws_s3_bucket" "ingestion_code_bucket" {
#   bucket = "nc-team-reveries-ingestion-code"
# }


resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-team-reveries-ingestion"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key = "ingestion_code/sql_utils.zip"       # ----- has to be changed 
  source = "${path.module}/../src/ingestion/utils/sql_utils.zip"       # --- has to be changed
}


# resource "aws_s3_bucket_versioning" "ingestion_bucket_versioning" {
#   bucket = aws_s3_bucket.ingestion_bucket_versioning.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }



resource "aws_s3_bucket_notification" "ingestion_lambda_trigger" {
  bucket = aws_s3_bucket.ingestion_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_lambda_handler.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.lambda_invoke]
}