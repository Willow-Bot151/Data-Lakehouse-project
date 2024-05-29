resource "aws_iam_role" "lambda_role" {
    name_prefix = "role-${var.lambda_name}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["*"]

    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.processing_bucket.arn}",
      "${aws_s3_bucket.processing_bucket.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "cw_document" {
  statement {

    actions = [ "logs:CreateLogGroup" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }

  statement {

    actions = [ "logs:CreateLogStream",  "logs:PutLogEvents" ]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_name}:*",
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.Processing_lambda}:*"
      #"arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/ingestion_lambda/logs:*"
    ]
  }
}

data "aws_iam_policy_document" "ingestion_secrets_policy_document" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
  
}

resource "aws_iam_policy" "s3_policy" {
    name_prefix = "s3-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.s3_document.json
}


resource "aws_iam_policy" "cw_policy" {
    name_prefix = "cw-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_policy" "secrets_policy" {
    name_prefix = "secrets-policy-${var.lambda_name}"
    policy = data.aws_iam_policy_document.ingestion_secrets_policy_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_cw_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}


resource "aws_iam_role_policy_attachment" "lambda_secrets_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.secrets_policy.arn
}


#----------------------------------------------------------------------------------------------------------------------------
#---------------------------PROCESSING-TERRAFORM-----------------------------------------------------------------------------
#----------------------------------------------------------------------------------------------------------------------------

resource "aws_iam_role" "processing_lambda_role" {
    name_prefix = "role-${var.Processing_lambda}"
    assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

#--------------------------------s3-POLICY---------------------------------------

data "aws_iam_policy_document" "processing_s3_document" {
  statement {

    actions = ["*"]

    resources = [
        "${aws_s3_bucket.ingestion_bucket.arn}",
        "${aws_s3_bucket.ingestion_bucket.arn}/*",
        "${aws_s3_bucket.processing_bucket.arn}",
        "${aws_s3_bucket.processing_bucket.arn}/*"
    ]
  }
}

resource "aws_iam_policy" "processing_s3_policy" {
    name_prefix = "s3-policy-${var.Processing_lambda}"
    policy = data.aws_iam_policy_document.processing_s3_document.json
}

resource "aws_iam_role_policy_attachment" "processing_lambda_s3_policy_attachment" {
    role = aws_iam_role.processing_lambda_role.name
    policy_arn = aws_iam_policy.processing_s3_policy.arn
}


resource "aws_iam_policy" "processing_cw_policy" {
    name_prefix = "cw-policy-${var.Processing_lambda}"
    policy = data.aws_iam_policy_document.cw_document.json
}

resource "aws_iam_role_policy_attachment" "processing_lambda_cw_policy_attachment" {
    role = aws_iam_role.processing_lambda_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}



# resource "aws_secretsmanager_secret" "get_aws_secrets_ingestion" {
#   name = "aws_secrets_ingestion"
# }



# resource "aws_sns_topic_policy" "default" {
#   arn = aws_sns_topic.lambda_errors.arn
#   policy = data.aws_iam_policy_document.sns_topic_policy.json
# }

# data "aws_iam_policy_document" "sns_topic_policy" {
#   policy_id = "__default_policy_ID"

#   statement {
#     actions = [
#       "SNS:Subscribe",
#       "SNS:SetTopicAttributes",
#       "SNS:RemovePermission",
#       "SNS:Receive",
#       "SNS:Publish",
#       "SNS:ListSubscriptionsByTopic",
#       "SNS:GetTopicAttributes",
#       "SNS:DeleteTopic",
#       "SNS:AddPermission",
#     ]

#     effect = "Allow"

#     principals {
#       type        = "AWS"
#       identifiers = ["*"]
#     }

#     resources = [
#       aws_sns_topic.lambda_errors.arn
#     ]
#   }
# }