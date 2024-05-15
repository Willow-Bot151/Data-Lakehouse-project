
# resource "aws_s3_bucket" "ingestion_code_bucket" {
#   bucket = "nc-team-reveries-ingestion-code"
# }


resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "nc-team-reveries-ingestion"
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.ingestion_bucket.bucket
  key = "s3_file_reader/function.zip"
  source = "${path.module}/../function.zip"
}


# resource "aws_s3_bucket_versioning" "ingestion_bucket_versioning" {
#   bucket = aws_s3_bucket.ingestion_bucket_versioning.id
#   versioning_configuration {
#     status = "Enabled"
#   }
# }



# resource "aws_s3_bucket_notification" "bucket_notification" {
#   bucket = aws_s3_bucket.ingestion_data_bucket.id

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.de-tote-sys.arn
#     events              = ["s3:ObjectCreated:*"]
#   }

#   depends_on = [aws_lambda_permission.allow_s3]
# }