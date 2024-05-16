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
  default = "python3.11"
}

# variable "source_file" {
#   type = string
#   default = "${path.module}/../src/ingestion/utils/sql_utils.py" 
# }

variable "source_file" {
  type = string
  default = "../src/ingestion/utils/sql_utils.py" 
}

variable "output_path" {
  type = string
  default = "../src/ingestion/utils/sql_utils.zip"
}


data "aws_caller_identity" "current" {}

data "aws_region" "current" {}