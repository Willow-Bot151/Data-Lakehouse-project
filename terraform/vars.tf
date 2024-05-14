variable "lambda_name" {
  type = string
  default = "ingestion_lambda_handler"
}

variable "aws_region" {
  type = string
  default = "eu-west-2"
}

variable "python_runtime" {
  type = string
  default = "python3.12"
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}