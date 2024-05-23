# This whole section may depend on error handling in python code
# subject to change dependent on error messages being logged

# resource "aws_cloudwatch_log_group" "ingestion_lambda_log_group" {
#   name = "/aws/lambda/ingestion_lambda"   # needs to change depending on location of cloudwatch logging
#   retention_in_days = 30  
# }


resource "aws_cloudwatch_log_metric_filter" "ingestion_lambda_error_messages" {
  name           = "IngestionErrorFilter"
  pattern        = "\"ERROR\""  # needs to be changed depending on error handling
  log_group_name = "/aws/lambda/ingestion_lambda" # aws_cloudwatch_log_group.ingestion_lambda_log_group.name
  metric_transformation {
      name = var.metric_transformation_name_error
      namespace = var.metric_namespace_error
      value = "1"
      default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_alarm" {
  metric_name               = var.metric_transformation_name_error
  alarm_name                = "Ingestion Error Alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 1
  statistic                 = "Sum"
  threshold = 1
  alarm_description         = "This metric monitors logging for ingestion errors"
  insufficient_data_actions = []
  alarm_actions       =     [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = var.metric_namespace_error
}

resource "aws_cloudwatch_log_metric_filter" "successful_ingestion_messages" {
  name = "SuccessfulIngestionFilter"
  pattern = "\"STARTPROCESSING\""
  log_group_name = "/aws/lambda/ingestion_lambda"
  metric_transformation {
    name = var.metric_transformation_name_success
    namespace = var.metric_namespace_success
    value = "1"
    default_value = "0"
  }
}

resource "aws_cloudwatch_metric_alarm" "ingestion_success_alarm" {
  metric_name = var.metric_transformation_name_success
  alarm_name = "SuccesfulIngestionAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods = 1
  statistic = "Sum"
  threshold = 1
  alarm_description = "Monitors ingestion success"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.lambda_errors.arn]
  period = 60
  namespace = var.metric_namespace_success
}

resource "aws_sns_topic" "lambda_errors" {
    name = "lambda_handler_notify"
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_1" {
  protocol  = "email"
  endpoint  = "annshelly@hotmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_2" {
  protocol  = "email"
  endpoint  = "laxmiprasanna.immadi@gmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_3" {
  protocol  = "email"
  endpoint  = "lukedowney2014@gmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_4" {
  protocol  = "email"
  endpoint  = "willowhart151@gmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}

resource "aws_sns_topic_subscription" "email_lambda_error_messages_5" {
  protocol  = "email"
  endpoint  = "cammcburney95@gmail.com"
  topic_arn = aws_sns_topic.lambda_errors.arn
}








